/*
Copyright 2026 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package xputopologyaware

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

// compiledTask is an immutable-by-convention Session-local view of the
// policies relevant to one Task. It never carries a DeviceKey selection: M2
// can describe a structural preference only.
type compiledTask struct {
	softPolicies []compiledPolicy
	blocked      *api.ValidateResult
}

type compiledPolicy struct {
	policy   api.CanonicalDeviceTopologyPolicy
	request  int64
	groupKey *groupPolicyKey
}

// groupPolicyKey identifies a Session-local Group soft preference. It is not
// persisted and intentionally contains no selected device identity.
type groupPolicyKey struct {
	job       api.JobID
	subJob    api.SubJobID
	policyKey string
}

type compiler struct {
	manager  topology.ProcessManager
	snapshot *api.DeviceTopologySnapshot
	now      func() time.Time

	compiledMu sync.Mutex
	compiled   map[api.TaskID]compiledTask
}

func newCompiler(manager topology.ProcessManager, snapshot *api.DeviceTopologySnapshot, now func() time.Time) *compiler {
	if now == nil {
		now = time.Now
	}
	return &compiler{
		manager:  manager,
		snapshot: snapshot,
		now:      now,
		compiled: make(map[api.TaskID]compiledTask),
	}
}

func (c *compiler) validateJob(job *api.JobInfo) *api.ValidateResult {
	if job == nil {
		return blocked("XPUTopologyPolicyInvalid", "xPU topology policy expected *api.JobInfo")
	}
	if !job.HasDeviceTopologyPolicy() {
		return nil
	}

	tasks := make([]*api.TaskInfo, 0, len(job.Tasks))
	for _, task := range job.Tasks {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].UID < tasks[j].UID })
	for _, task := range tasks {
		// A policy mutation takes effect only for a future, not-yet-bound Pod.
		// Allocated and Pipelined tasks can still enter Bind during this Session;
		// Binding/Bound/Running (and terminal) tasks must not be retroactively
		// invalidated by an updated PodGroup policy.
		if !taskNeedsM2Compilation(task) {
			continue
		}
		compiled := c.compileTask(job, task)
		if compiled.blocked != nil {
			return compiled.blocked
		}
	}
	return nil
}

func (c *compiler) compileTask(job *api.JobInfo, task *api.TaskInfo) compiledTask {
	if task == nil {
		return compiledTask{blocked: blocked(api.XPUTopologyPolicyInvalidReason, "xPU topology policy selected a nil Task")}
	}
	c.compiledMu.Lock()
	defer c.compiledMu.Unlock()
	if compiled, found := c.compiled[task.UID]; found {
		return compiled
	}

	result := c.compileTaskUncached(job, task)
	c.compiled[task.UID] = result
	return result
}

func taskNeedsM2Compilation(task *api.TaskInfo) bool {
	if task == nil {
		return true
	}
	switch task.Status {
	case api.Pending, api.Pipelined, api.Allocated:
		return true
	default:
		return false
	}
}

func (c *compiler) compileTaskUncached(job *api.JobInfo, task *api.TaskInfo) compiledTask {
	if job == nil {
		return compiledTask{blocked: blocked(api.XPUTopologyPolicyInvalidReason, "Task %s has no JobInfo", task.UID)}
	}
	if !job.HasDeviceTopologyPolicy() {
		return compiledTask{}
	}
	if !job.DeviceTopology.Empty() && !job.DeviceTopologyValid {
		return compiledTask{blocked: blocked(api.XPUTopologyPolicyInvalidReason, "PodGroup policy for job %s is not canonical", job.UID)}
	}

	policies := make([]compiledPolicy, 0, len(job.DeviceTopology.Policies))
	for _, policy := range job.DeviceTopology.Policies {
		if policyAppliesToTask(policy, task) {
			policies = append(policies, compiledPolicy{policy: policy, groupKey: groupKeyFor(job, "", policy)})
		}
	}

	if subJobID, found := job.TaskToSubJob[task.UID]; found {
		if subJob, found := job.SubJobs[subJobID]; found && subJob != nil {
			if !subJob.DeviceTopology.Empty() && !subJob.DeviceTopologyValid {
				return compiledTask{blocked: blocked(api.XPUTopologyPolicyInvalidReason, "SubGroup policy %s for job %s is not canonical", subJob.UID, job.UID)}
			}
			for _, policy := range subJob.DeviceTopology.Policies {
				if policyAppliesToTask(policy, task) {
					policies = append(policies, compiledPolicy{policy: policy, groupKey: groupKeyFor(job, subJob.UID, policy)})
				}
			}
		}
	}
	if len(policies) == 0 {
		return compiledTask{}
	}

	activation, activationResult := c.activationForPolicies()
	if activationResult != nil {
		return compiledTask{blocked: activationResult}
	}
	catalog := c.manager.Catalog()
	if catalog == nil {
		return compiledTask{blocked: blocked(string(topology.CatalogNotReady), "xPU topology catalog is not ready")}
	}

	softPolicies := make([]compiledPolicy, 0, len(policies))
	for index := range policies {
		compiled := policies[index]
		if !catalog.HasDomainClass(compiled.policy.DomainClass) {
			return compiledTask{blocked: blocked(api.XPUTopologyDomainClassUnknownReason,
				"resource=%s scope=%s domainClass=%s is absent from the fixed catalog",
				compiled.policy.ResourceName, compiled.policy.DomainClass.Scope, compiled.policy.DomainClass.Name)}
		}
		if c.snapshot != nil && c.snapshot.ProviderCapabilitiesKnown {
			if _, supported := c.snapshot.SupportedProviderDomainClasses[compiled.policy.DomainClass]; !supported {
				return compiledTask{blocked: blocked(api.XPUTopologyDomainClassUnsupportedReason,
					"resource=%s scope=%s domainClass=%s is not declared by the configured Provider",
					compiled.policy.ResourceName, compiled.policy.DomainClass.Scope, compiled.policy.DomainClass.Name)}
			}
		}

		request, err := taskResourceRequest(task, compiled.policy.ResourceName)
		if err != nil {
			return compiledTask{blocked: blocked(api.XPUTopologyUnsupportedPodRequestReason,
				"Task %s for resource %s: %v", task.UID, compiled.policy.ResourceName, err)}
		}
		compiled.request = request

		if compiled.policy.Mode == scheduling.HardDeviceTopologyMode {
			reason := activation.PolicyReason(true)
			return compiledTask{blocked: blocked(string(reason),
				"resource=%s scope=%s domainClass=%s cannot be enforced by Advisory M2",
				compiled.policy.ResourceName, compiled.policy.DomainClass.Scope, compiled.policy.DomainClass.Name)}
		}
		softPolicies = append(softPolicies, compiled)
	}
	return compiledTask{softPolicies: softPolicies}
}

// activationForPolicies preserves the fail-closed double opt-in and fixed
// catalog contract. ProviderNotReady is deliberately not a soft blocker: a
// valid soft policy merely has no topology evidence and contributes zero.
func (c *compiler) activationForPolicies() (topology.Activation, *api.ValidateResult) {
	if c.manager == nil {
		return topology.Activation{}, blocked(string(topology.FeatureDisabled), "xPU topology manager is unavailable")
	}
	activation := c.manager.Activation()
	switch reason := activation.Reason(); reason {
	case topology.ActivationReady, topology.ProviderNotReady:
		return activation, nil
	default:
		return activation, blocked(string(reason), "xPU topology policy cannot be consumed while activation is %s", reason)
	}
}

func policyAppliesToTask(policy api.CanonicalDeviceTopologyPolicy, task *api.TaskInfo) bool {
	if policy.ApplyTo != scheduling.DeviceTopologyApplyToPod || policy.PodSelector.Empty() {
		return true
	}
	if task == nil || task.Pod == nil {
		return false
	}
	return selectorMatches(policy.PodSelector, task.Pod.Labels)
}

func selectorMatches(selector api.CanonicalLabelSelector, values map[string]string) bool {
	for _, label := range selector.MatchLabels {
		if values[label.Key] != label.Value {
			return false
		}
	}
	for _, requirement := range selector.MatchExpressions {
		value, present := values[requirement.Key]
		hasValue := false
		for _, expected := range requirement.Values {
			if value == expected {
				hasValue = true
				break
			}
		}
		switch requirement.Operator {
		case metav1.LabelSelectorOpIn:
			if !present || !hasValue {
				return false
			}
		case metav1.LabelSelectorOpNotIn:
			if present && hasValue {
				return false
			}
		case metav1.LabelSelectorOpExists:
			if !present {
				return false
			}
		case metav1.LabelSelectorOpDoesNotExist:
			if present {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func taskResourceRequest(task *api.TaskInfo, resourceName corev1.ResourceName) (int64, error) {
	if task == nil || task.Pod == nil {
		return 0, fmt.Errorf("Pod is required")
	}
	pod := task.Pod
	if len(task.DRAResreq) != 0 || len(task.ResourceClaimKeys) != 0 || len(task.ResourceClaimDRAResreq) != 0 {
		return 0, fmt.Errorf("M2 does not support DRA requests")
	}

	var (
		matched       bool
		matchedValue  int64
		matchedSource string
	)
	checkContainer := func(container corev1.Container, source string) error {
		request, requested := container.Resources.Requests[resourceName]
		limit, limited := container.Resources.Limits[resourceName]
		if !requested && !limited {
			return nil
		}

		if matched {
			return fmt.Errorf("only one container may request device resource %s; already found in %s, also found in %s %q", resourceName, matchedSource, source, container.Name)
		}
		if !limited {
			return fmt.Errorf("%s %q: device resource limit must be set", source, container.Name)
		}
		if limit.Sign() <= 0 || limit.MilliValue()%1000 != 0 {
			return fmt.Errorf("%s %q: limit must be a positive integral device count", source, container.Name)
		}
		if requested && request.Cmp(limit) != 0 {
			return fmt.Errorf("%s %q: request must equal limit when both are set", source, container.Name)
		}

		matched = true
		matchedValue = limit.Value()
		matchedSource = fmt.Sprintf("%s %q", source, container.Name)
		return nil
	}

	for _, container := range pod.Spec.Containers {
		if err := checkContainer(container, "regular container"); err != nil {
			return 0, err
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if err := checkContainer(container, "init container"); err != nil {
			return 0, err
		}
	}
	if !matched {
		return 0, fmt.Errorf("device resource %s must be requested by exactly one container", resourceName)
	}
	return matchedValue, nil
}

func groupKeyFor(job *api.JobInfo, subJob api.SubJobID, policy api.CanonicalDeviceTopologyPolicy) *groupPolicyKey {
	if policy.ApplyTo != scheduling.DeviceTopologyApplyToGroup {
		return nil
	}
	return &groupPolicyKey{
		job:       job.UID,
		subJob:    subJob,
		policyKey: canonicalPolicyKey(policy),
	}
}

func canonicalPolicyKey(policy api.CanonicalDeviceTopologyPolicy) string {
	return strings.Join([]string{
		string(policy.ResourceName),
		string(policy.ApplyTo),
		string(policy.Mode),
		string(policy.DomainClass.Scope),
		string(policy.DomainClass.Name),
	}, "\x00")
}

func blocked(reason, format string, args ...interface{}) *api.ValidateResult {
	return &api.ValidateResult{Pass: false, Reason: reason, Message: fmt.Sprintf(format, args...)}
}
