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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	schedulingapi "volcano.sh/apis/pkg/apis/scheduling"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

const compilerTestResource corev1.ResourceName = "nvidia.com/gpu"

func TestCompilerBlocksHardPolicyWithoutAssignmentContract(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("hard", scheduling.HardDeviceTopologyMode, "local-scale-up", 2)
	compiler := newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 2}, nil), func() time.Time { return now })

	result := compiler.compileTask(job, task)
	if result.blocked == nil || result.blocked.Reason != string(topology.AssignmentNotEnforceable) {
		t.Fatalf("hard compiler result = %#v, want %s blocker", result, topology.AssignmentNotEnforceable)
	}
	if len(result.softPolicies) != 0 {
		t.Fatalf("hard policy compiled soft policies: %#v", result.softPolicies)
	}
}

func TestCompilerRejectsUnknownCatalogClass(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("unknown", scheduling.SoftDeviceTopologyMode, "not-in-catalog", 1)
	compiler := newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1}, nil), func() time.Time { return now })

	result := compiler.compileTask(job, task)
	if result.blocked == nil || result.blocked.Reason != api.XPUTopologyDomainClassUnknownReason {
		t.Fatalf("unknown class result = %#v, want %s", result, api.XPUTopologyDomainClassUnknownReason)
	}
}

func TestCompilerRejectsKnownClassUnsupportedByProvider(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("unsupported", scheduling.SoftDeviceTopologyMode, "scale-up-fabric", 1)
	job.DeviceTopology.Policies[0].DomainClass.Scope = scheduling.DeviceTopologyDomainScopeFabric
	snapshot := compilerTestSnapshot(map[string]int64{"node-a": 1}, nil)
	snapshot.ProviderCapabilitiesKnown = true
	snapshot.SupportedProviderDomainClasses = map[api.DomainClassKey]struct{}{
		{ResourceName: compilerTestResource, Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}: {},
	}
	compiler := newCompiler(manager, snapshot, func() time.Time { return now })

	result := compiler.compileTask(job, task)
	if result.blocked == nil || result.blocked.Reason != api.XPUTopologyDomainClassUnsupportedReason {
		t.Fatalf("unsupported class result = %#v, want %s", result, api.XPUTopologyDomainClassUnsupportedReason)
	}
}

func TestSoftScoreUsesHealthyStructuralCapacityAndCompactOrder(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("soft", scheduling.SoftDeviceTopologyMode, "local-scale-up", 2)
	compiler := newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-b": 4, "node-a": 2}, nil), func() time.Time { return now })
	scorer := newSessionScorer(compiler)
	nodes := []*api.NodeInfo{{Name: "node-b"}, {Name: "node-a"}, {Name: "node-without-facts"}}

	scores := scorer.score(task, nodes, job)
	if scores["node-a"] <= scores["node-b"] {
		t.Fatalf("scores = %#v, want exact-fit node-a preferred over node-b", scores)
	}
	if _, found := scores["node-without-facts"]; found {
		t.Fatalf("missing facts must produce zero contribution, got %#v", scores)
	}
	if scores["node-a"] > MaxAdvisoryScore || scores["node-b"] > MaxAdvisoryScore {
		t.Fatalf("scores exceed advisory bound: %#v", scores)
	}
	// Candidate order is Predicate-owned and may vary; relative contribution
	// must not vary with that input ordering.
	reversed := scorer.score(task, []*api.NodeInfo{{Name: "node-without-facts"}, {Name: "node-a"}, {Name: "node-b"}}, job)
	if !reflect.DeepEqual(scores, reversed) {
		t.Fatalf("candidate order changed deterministic scores: first=%#v reversed=%#v", scores, reversed)
	}
}

func TestSoftFabricScoreRequiresExplicitProviderMembership(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("fabric", scheduling.SoftDeviceTopologyMode, "scale-up-fabric", 1)
	job.DeviceTopology.Policies[0].DomainClass.Scope = scheduling.DeviceTopologyDomainScopeFabric
	nodes := []*api.NodeInfo{{Name: "node-a"}, {Name: "node-b"}, {Name: "node-c"}}

	// Two Nodes alone do not become a Fabric by inference.
	implicit := newSessionScorer(newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1, "node-b": 1, "node-c": 1}, nil), func() time.Time { return now }))
	if scores := implicit.score(task, nodes, job); len(scores) != 0 {
		t.Fatalf("implicit Fabric received a score: %#v", scores)
	}

	explicit := newSessionScorer(newCompiler(manager, compilerTestFabricSnapshot(), func() time.Time { return now }))
	scores := explicit.score(task, nodes, job)
	if scores["node-a"] == 0 || scores["node-b"] == 0 {
		t.Fatalf("explicit Fabric members did not receive a score: %#v", scores)
	}
	if _, found := scores["node-c"]; found {
		t.Fatalf("non-member node received explicit Fabric score: %#v", scores)
	}
}

func TestSoftFabricScoreUsesNodeLocalCapacity(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("fabric-local-capacity", scheduling.SoftDeviceTopologyMode, "scale-up-fabric", 2)
	job.DeviceTopology.Policies[0].DomainClass.Scope = scheduling.DeviceTopologyDomainScopeFabric
	snapshot := compilerTestFabricSnapshotWithCapacities(map[string]int64{"node-a": 1, "node-b": 3})
	scorer := newSessionScorer(newCompiler(manager, snapshot, func() time.Time { return now }))

	scores := scorer.score(task, []*api.NodeInfo{{Name: "node-a"}, {Name: "node-b"}}, job)
	if _, found := scores["node-a"]; found {
		t.Fatalf("node-a received a Fabric score from aggregate capacity: %#v", scores)
	}
	if scores["node-b"] <= 0 {
		t.Fatalf("node-b did not receive a Fabric score from its local capacity: %#v", scores)
	}
}

func TestNoPolicyContributesNoScore(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	_, task := compilerTestJob("no-policy", scheduling.SoftDeviceTopologyMode, "local-scale-up", 1)
	job := api.NewJobInfo("default/no-policy", task)
	task.Job = job.UID
	scorer := newSessionScorer(newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1}, nil), func() time.Time { return now }))
	nodes := []*api.NodeInfo{{Name: "node-a"}}
	if scores := scorer.score(task, nodes, job); len(scores) != 0 {
		t.Fatalf("unconfigured job received xPU score: %#v", scores)
	}
	if len(nodes) != 1 || nodes[0].Name != "node-a" {
		t.Fatalf("unconfigured job changed Predicate candidates: %#v", nodes)
	}
}

func TestSoftScoreDropsExpiredFactsWithoutFilteringCandidate(t *testing.T) {
	now := time.Unix(100, 0)
	expired := now.Add(-time.Second)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("stale", scheduling.SoftDeviceTopologyMode, "local-scale-up", 1)
	compiler := newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1}, map[string]time.Time{"node-a": expired}), func() time.Time { return now })
	scorer := newSessionScorer(compiler)
	nodes := []*api.NodeInfo{{Name: "node-a"}}

	scores := scorer.score(task, nodes, job)
	if len(scores) != 0 {
		t.Fatalf("expired facts must contribute zero rather than a preference: %#v", scores)
	}
	// The candidate slice is owned by Predicate; scoring did not mutate it or
	// filter the stale node out.
	if len(nodes) != 1 || nodes[0].Name != "node-a" {
		t.Fatalf("soft score changed Predicate candidates: %#v", nodes)
	}
}

func TestGroupSoftOverlayPrefersSessionPlacementAndReleasesIt(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, first := compilerTestJob("group-0", scheduling.SoftDeviceTopologyMode, "local-scale-up", 1)
	job.DeviceTopology.Policies[0].ApplyTo = scheduling.DeviceTopologyApplyToGroup
	second := compilerTestTask("group-1", 1)
	second.Job = job.UID
	job.AddTaskInfo(second)
	compiler := newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1, "node-b": 1}, nil), func() time.Time { return now })
	scorer := newSessionScorer(compiler)
	nodes := []*api.NodeInfo{{Name: "node-a"}, {Name: "node-b"}}

	first.NodeName = "node-b"
	scorer.allocated(first, job)
	scores := scorer.score(second, nodes, job)
	if scores["node-b"] <= scores["node-a"] {
		t.Fatalf("group overlay did not prefer first Session placement: %#v", scores)
	}

	scorer.deallocated(first, job)
	scores = scorer.score(second, nodes, job)
	if scores["node-a"] <= scores["node-b"] {
		t.Fatalf("removing overlay did not restore deterministic Compact order: %#v", scores)
	}
}

func TestCompilerIgnoresUpdatedPolicyForAlreadyRunningTask(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("running", scheduling.SoftDeviceTopologyMode, "local-scale-up", 1)
	task.Status = api.Running
	// An updated policy may no longer match an already-running Pod shape. It
	// must affect only future, unbound Tasks.
	task.Pod.Spec.Containers = append(task.Pod.Spec.Containers, corev1.Container{Name: "sidecar"})
	compiler := newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1}, nil), func() time.Time { return now })

	if result := compiler.validateJob(job); result != nil {
		t.Fatalf("running task was retroactively blocked: %#v", result)
	}
}

func TestValidationReporterPublishesAndResolvesRuntimeXPUReason(t *testing.T) {
	job, _ := compilerTestJob("status", scheduling.SoftDeviceTopologyMode, "local-scale-up", 1)
	job.SetPodGroup(&api.PodGroup{PodGroup: schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "status"},
		Spec:       schedulingapi.PodGroupSpec{MinMember: 1},
	}})
	ssn := &framework.Session{UID: types.UID("session"), Jobs: map[api.JobID]*api.JobInfo{job.UID: job}}
	reporter := newValidationReporter(ssn)

	reporter.report(job, blocked(api.XPUTopologyDomainClassUnknownReason, "unknown class"))
	conditions := job.PodGroup.Status.Conditions
	if len(conditions) != 1 || conditions[0].Status != corev1.ConditionTrue || conditions[0].Reason != api.XPUTopologyDomainClassUnknownReason {
		t.Fatalf("runtime condition = %#v", conditions)
	}
	reporter.report(job, nil)
	conditions = job.PodGroup.Status.Conditions
	if len(conditions) != 1 || conditions[0].Status != corev1.ConditionFalse || conditions[0].Reason != api.XPUTopologyResolvedReason {
		t.Fatalf("resolved runtime condition = %#v", conditions)
	}
}

func TestSessionScorerIsSafeForConcurrentCallbacks(t *testing.T) {
	now := time.Unix(100, 0)
	manager := compilerTestManager(t)
	job, task := compilerTestJob("concurrent", scheduling.SoftDeviceTopologyMode, "local-scale-up", 1)
	job.DeviceTopology.Policies[0].ApplyTo = scheduling.DeviceTopologyApplyToGroup
	scorer := newSessionScorer(newCompiler(manager, compilerTestSnapshot(map[string]int64{"node-a": 1}, nil), func() time.Time { return now }))
	nodes := []*api.NodeInfo{{Name: "node-a"}}
	task.NodeName = "node-a"

	var workers sync.WaitGroup
	for index := 0; index < 16; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_ = scorer.score(task, nodes, job)
			scorer.allocated(task, job)
			scorer.deallocated(task, job)
		}()
	}
	workers.Wait()
}

func compilerTestManager(t *testing.T) *topology.Manager {
	t.Helper()
	catalog, err := topology.LoadCatalog(strings.NewReader(`{
  "resources":[{
    "resourceName":"nvidia.com/gpu",
    "domainClasses":[
      {"scope":"Node","name":"local-scale-up"},
      {"scope":"Fabric","name":"scale-up-fabric"}
    ]
  }]
}`))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	manager := topology.NewManager()
	if err := manager.Configure(topology.ActivationConfig{
		GateEnabled: true, PluginConfigured: true, PluginConfigReady: true, ConfigurationIdentity: "compiler-test",
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if err := manager.ConfigureCatalog(catalog); err != nil {
		t.Fatalf("ConfigureCatalog() error = %v", err)
	}
	manager.SetReadiness(topology.Readiness{ProviderReady: true})
	return manager
}

func compilerTestJob(name string, mode scheduling.DeviceTopologyMode, domainClass scheduling.DeviceTopologyDomainClass, devices int64) (*api.JobInfo, *api.TaskInfo) {
	task := compilerTestTask(name, devices)
	job := api.NewJobInfo(api.JobID("default/"+name), task)
	task.Job = job.UID
	job.DeviceTopology = api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
		ResourceName: compilerTestResource,
		ApplyTo:      scheduling.DeviceTopologyApplyToPod,
		Mode:         mode,
		DomainClass: api.DomainClassKey{
			ResourceName: compilerTestResource,
			Scope:        scheduling.DeviceTopologyDomainScopeNode,
			Name:         domainClass,
		},
	}}}
	job.DeviceTopologyValid = true
	return job, task
}

func compilerTestTask(name string, devices int64) *api.TaskInfo {
	quantity := *resource.NewQuantity(devices, resource.DecimalSI)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "worker",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{compilerTestResource: quantity},
				Limits:   corev1.ResourceList{compilerTestResource: quantity},
			},
		}}},
	}
	return api.NewTaskInfo(pod)
}

func TestTaskResourceRequestSupportsMultipleContainersAndInitContainers(t *testing.T) {
	quantity := *resource.NewQuantity(2, resource.DecimalSI)
	restartPolicy := corev1.ContainerRestartPolicyAlways

	tests := []struct {
		name           string
		containers     []corev1.Container
		initContainers []corev1.Container
		want           int64
	}{
		{
			name: "multiple regular containers with one device consumer",
			containers: []corev1.Container{
				{Name: "worker", Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{compilerTestResource: quantity},
				}},
				{Name: "sidecar"},
			},
			want: 2,
		},
		{
			name:       "ordinary init container with device limit",
			containers: []corev1.Container{{Name: "worker"}},
			initContainers: []corev1.Container{{Name: "prepare", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{compilerTestResource: quantity},
			}}},
			want: 2,
		},
		{
			name:       "restartable init container with device limit",
			containers: []corev1.Container{{Name: "worker"}},
			initContainers: []corev1.Container{{Name: "sidecar-init", RestartPolicy: &restartPolicy, Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{compilerTestResource: quantity},
			}}},
			want: 2,
		},
		{
			name: "request and limit are both accepted when equal",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{compilerTestResource: quantity},
				Limits:   corev1.ResourceList{compilerTestResource: quantity},
			}}},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{
				Containers:     tt.containers,
				InitContainers: tt.initContainers,
			}}
			got, err := taskResourceRequest(api.NewTaskInfo(pod), compilerTestResource)
			if err != nil {
				t.Fatalf("taskResourceRequest() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("taskResourceRequest() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestTaskResourceRequestRejectsUnsupportedDeviceShapes(t *testing.T) {
	integerTwo := *resource.NewQuantity(2, resource.DecimalSI)
	fractional := *resource.NewMilliQuantity(1500, resource.DecimalSI)
	tests := []struct {
		name       string
		containers []corev1.Container
		wantError  string
	}{
		{
			name: "two regular containers request the device",
			containers: []corev1.Container{
				{Name: "worker", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{compilerTestResource: integerTwo}}},
				{Name: "other", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{compilerTestResource: integerTwo}}},
			},
			wantError: "only one container",
		},
		{
			name: "request without limit",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{compilerTestResource: integerTwo},
			}}},
			wantError: "limit must be set",
		},
		{
			name: "fractional limit",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{compilerTestResource: fractional},
			}}},
			wantError: "positive integral",
		},
		{
			name: "request and limit differ",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{compilerTestResource: *resource.NewQuantity(1, resource.DecimalSI)},
				Limits:   corev1.ResourceList{compilerTestResource: integerTwo},
			}}},
			wantError: "request must equal limit",
		},
		{
			name:       "no container requests the device",
			containers: []corev1.Container{{Name: "worker"}},
			wantError:  "must be requested by exactly one container",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: tt.containers}}
			_, err := taskResourceRequest(api.NewTaskInfo(pod), compilerTestResource)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("taskResourceRequest() error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func compilerTestSnapshot(capacities map[string]int64, freshUntil map[string]time.Time) *api.DeviceTopologySnapshot {
	class := api.DomainClassKey{ResourceName: compilerTestResource, Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}
	nodes := make(map[string]api.DeviceTopologyNodeState, len(capacities))
	devices := map[api.DeviceKey]api.TopologyDevice{}
	domains := map[api.LocalDomainKey]api.DeviceDomain{}
	domainsByClass := map[api.DomainClassKey][]api.LocalDomainKey{}

	for nodeName, capacity := range capacities {
		uid := types.UID("uid-" + nodeName)
		nodes[nodeName] = api.DeviceTopologyNodeState{
			Identity:         api.NodeIdentity{Name: nodeName, UID: uid},
			SyncState:        api.DeviceTopologyNodeSynced,
			SourceGeneration: 1,
			FreshUntil:       freshUntil[nodeName],
		}
		domainKey := api.LocalDomainKey{
			ResourceName: compilerTestResource,
			OwnerNodeUID: uid,
			ID:           api.DomainID{ProviderID: "mock", Namespace: "example.com", Value: api.SourceDomainID("domain-" + nodeName)},
		}
		deviceKeys := make([]api.DeviceKey, 0, capacity)
		for index := int64(0); index < capacity; index++ {
			deviceKey := api.DeviceKey{
				ResourceName: compilerTestResource,
				OwnerNodeUID: uid,
				ID:           api.DeviceID{ProviderID: "mock", Namespace: "example.com", Value: api.SourceDeviceID(fmt.Sprintf("%s-%d", nodeName, index))},
			}
			deviceKeys = append(deviceKeys, deviceKey)
			devices[deviceKey] = api.TopologyDevice{Key: deviceKey, NodeName: nodeName, Health: api.DeviceHealthy, SourceGeneration: 1}
		}
		domains[domainKey] = api.DeviceDomain{
			Key: domainKey, Class: class, NodeName: nodeName, MemberDeviceKeys: deviceKeys, EffectiveDeviceKeys: deviceKeys, SourceGeneration: 1,
		}
		domainsByClass[class] = append(domainsByClass[class], domainKey)
	}
	return api.NewDeviceTopologySnapshot(nodes, devices, domains, nil, domainsByClass, nil, nil)
}

func compilerTestFabricSnapshot() *api.DeviceTopologySnapshot {
	return compilerTestFabricSnapshotWithCapacities(map[string]int64{"node-a": 1, "node-b": 1, "node-c": 1})
}

func compilerTestFabricSnapshotWithCapacities(capacities map[string]int64) *api.DeviceTopologySnapshot {
	snapshot := compilerTestSnapshot(capacities, nil)
	class := api.DomainClassKey{ResourceName: compilerTestResource, Scope: scheduling.DeviceTopologyDomainScopeFabric, Name: "scale-up-fabric"}
	members := make([]api.FabricMember, 0, 2)
	for _, nodeName := range []string{"node-a", "node-b"} {
		state := snapshot.Nodes[nodeName]
		localDomains := make([]api.LocalDomainKey, 0, 1)
		for key, domain := range snapshot.LocalDomains {
			if domain.NodeName == nodeName {
				localDomains = append(localDomains, key)
			}
		}
		members = append(members, api.FabricMember{
			NodeName: nodeName, NodeUID: state.Identity.UID, SourceGeneration: state.SourceGeneration, LocalDomainKeys: localDomains,
		})
	}
	key := api.FabricKey{ProviderID: "mock", Namespace: "example.com", ResourceName: compilerTestResource, Value: "fabric-a-b"}
	snapshot.Fabrics[key] = api.FabricDomain{Key: key, Class: class, OwnerNodeUID: snapshot.Nodes["node-a"].Identity.UID, SourceGeneration: 1, Members: members}
	snapshot.FabricsByClass[class] = []api.FabricKey{key}
	return snapshot
}
