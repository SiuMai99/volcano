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
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName is the explicit scheduler plugin opt-in for xPU topology.
	PluginName = "xpu-topology-aware"

	// ProviderArgument selects the source-specific facts and assignment
	// confirmer. The first production Provider is annotation; mock is only
	// accepted for tests and fixtures.
	ProviderArgument = "xpu-topology.provider"
	// CatalogPathArgument identifies the fixed, scheduler-local catalog file.
	CatalogPathArgument = "xpu-topology.catalog"

	AnnotationProvider = "annotation"
	MockProvider       = "mock"
)

// Config is the static, Session-local copy of plugin arguments. It contains
// no live Provider or allocation state.
type Config struct {
	Provider    string
	CatalogPath string

	ProviderValid  bool
	CatalogPresent bool
}

// Valid reports whether the required static Provider identity is usable. A
// missing catalog remains a readiness failure, not a reason to silently fall
// back to ordinary scheduling.
func (c Config) Valid() bool {
	return c.ProviderValid
}

// Identity is stable for equivalent static plugin arguments and is used by the
// scheduler activation guard to reject identity changes during reload.
func (c Config) Identity() string {
	return c.Provider + "\x00" + c.CatalogPath
}

// ParseArguments follows the existing plugin argument contract: establish
// safe defaults, use Arguments.GetString for parsing, and mark unsafe values
// unavailable instead of panicking or starting a Provider.
func ParseArguments(arguments framework.Arguments) Config {
	config := Config{}
	arguments.GetString(&config.Provider, ProviderArgument)
	arguments.GetString(&config.CatalogPath, CatalogPathArgument)

	config.ProviderValid = config.Provider == AnnotationProvider || config.Provider == MockProvider
	config.CatalogPresent = strings.TrimSpace(config.CatalogPath) != ""
	return config
}

// ConfigFromPluginOption parses the arguments held by the scheduler config.
// It is shared with the scheduler activation guard so New and startup checks
// cannot disagree about static argument validity.
func ConfigFromPluginOption(option conf.PluginOption) Config {
	return ParseArguments(framework.Arguments(option.Arguments))
}

// New constructs only Session-local state. In particular, it never starts a
// process manager, Provider, worker, catalog reader, or external client.
func New(arguments framework.Arguments) framework.Plugin {
	config := ParseArguments(arguments)
	if !config.Valid() {
		klog.Warningf("%s plugin has an invalid %s argument; xPU policy will remain fail-closed", PluginName, ProviderArgument)
	}
	return &plugin{config: config, now: time.Now}
}

type plugin struct {
	config Config
	now    func() time.Time
}

// validationReporter owns only this Session's runtime xPU outcome condition.
// Authoring conditions remain protected by api.MergePodGroupConditions. The
// reporter deduplicates repeated JobValid calls so a static compiler failure
// does not continuously churn PodGroup status timestamps.
type validationReporter struct {
	ssn *framework.Session

	mu   sync.Mutex
	last map[api.JobID]string
}

func newValidationReporter(ssn *framework.Session) *validationReporter {
	return &validationReporter{ssn: ssn, last: make(map[api.JobID]string)}
}

func (r *validationReporter) report(job *api.JobInfo, result *api.ValidateResult) {
	if r == nil || r.ssn == nil || job == nil || job.PodGroup == nil {
		return
	}

	var condition *scheduling.PodGroupCondition
	stateKey := ""
	if result != nil && !result.Pass {
		stateKey = result.Reason + "\x00" + result.Message
		condition = &scheduling.PodGroupCondition{
			Type:               scheduling.PodGroupUnschedulableType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			TransitionID:       string(r.ssn.UID),
			Reason:             result.Reason,
			Message:            result.Message,
		}
	} else if hasRuntimeXPUBlocker(job.PodGroup.Status.Conditions) {
		stateKey = api.XPUTopologyResolvedReason
		condition = &scheduling.PodGroupCondition{
			Type:               scheduling.PodGroupUnschedulableType,
			Status:             corev1.ConditionFalse,
			LastTransitionTime: metav1.Now(),
			TransitionID:       string(r.ssn.UID),
			Reason:             api.XPUTopologyResolvedReason,
		}
	}
	if condition == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.last[job.UID] == stateKey {
		return
	}
	if err := r.ssn.UpdatePodGroupCondition(job, condition); err != nil {
		klog.Errorf("Failed to report xPU topology validation for job <%s/%s>: %v", job.Namespace, job.Name, err)
		return
	}
	r.last[job.UID] = stateKey
}

func hasRuntimeXPUBlocker(conditions []scheduling.PodGroupCondition) bool {
	for _, condition := range conditions {
		if condition.Type == scheduling.PodGroupUnschedulableType &&
			condition.Status == corev1.ConditionTrue &&
			!api.IsXPUTopologyAuthoringBlocker(condition) &&
			strings.HasPrefix(condition.Reason, "XPU") {
			return true
		}
	}
	return false
}

func (p *plugin) Name() string {
	return PluginName
}

// OnSessionOpen consumes only the immutable DeviceTopology pointer already
// paired with this Session's SchedulerCache snapshot. It neither starts the
// process-scoped manager nor reads live Provider state.
func (p *plugin) OnSessionOpen(ssn *framework.Session) {
	if ssn == nil {
		return
	}
	compiler := newCompiler(ssn.XPUTopologyManager(), ssn.DeviceTopology, p.now)
	scorer := newSessionScorer(compiler)
	reporter := newValidationReporter(ssn)

	ssn.AddJobValidFn(p.Name(), func(obj interface{}) *api.ValidateResult {
		job, ok := obj.(*api.JobInfo)
		if !ok {
			return blocked(api.XPUTopologyPolicyInvalidReason, "xPU topology policy expected *api.JobInfo, got %T", obj)
		}
		result := compiler.validateJob(job)
		reporter.report(job, result)
		return result
	})
	ssn.AddBatchNodeOrderFn(p.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		job, found := ssn.Jobs[task.Job]
		if !found {
			return map[string]float64{}, nil
		}
		return scorer.score(task, nodes, job), nil
	})
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			if event == nil || event.Task == nil {
				return
			}
			if job, found := ssn.Jobs[event.Task.Job]; found {
				scorer.allocated(event.Task, job)
			}
		},
		DeallocateFunc: func(event *framework.Event) {
			if event == nil || event.Task == nil {
				return
			}
			if job, found := ssn.Jobs[event.Task.Job]; found {
				scorer.deallocated(event.Task, job)
			}
		},
	})
}

// OnSessionClose intentionally does not stop or mutate process-scoped state.
func (p *plugin) OnSessionClose(_ *framework.Session) {}

// PluginOptions returns the xPU plugin options in tier order. It is kept here
// so the scheduler guard has one place to define duplicate/identity handling.
func PluginOptions(tiers []conf.Tier) []conf.PluginOption {
	var options []conf.PluginOption
	for _, tier := range tiers {
		for _, option := range tier.Plugins {
			if option.Name == PluginName {
				options = append(options, option)
			}
		}
	}
	return options
}

// ConfigIdentity returns an order-independent identity for configured xPU
// plugin options. Duplicate entries are retained as separate identities so a
// reload cannot silently change the number of xPU plugin instances.
func ConfigIdentity(options []conf.PluginOption) string {
	identities := make([]string, 0, len(options))
	for _, option := range options {
		identities = append(identities, ConfigFromPluginOption(option).Identity())
	}
	sort.Strings(identities)
	return strings.Join(identities, "\x01")
}
