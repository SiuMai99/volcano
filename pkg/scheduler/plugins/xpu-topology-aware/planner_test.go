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
	"math/rand"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

const plannerGPU corev1.ResourceName = "nvidia.com/gpu"
const plannerNPU corev1.ResourceName = "huawei.com/Ascend910"

var plannerNow = time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)

type plannerFixture struct {
	nodes          map[string]api.DeviceTopologyNodeState
	devices        map[api.DeviceKey]api.TopologyDevice
	domains        map[api.LocalDomainKey]api.DeviceDomain
	fabrics        map[api.FabricKey]api.FabricDomain
	localsByClass  map[api.DomainClassKey][]api.LocalDomainKey
	fabricsByClass map[api.DomainClassKey][]api.FabricKey
}

func newPlannerFixture(names ...string) *plannerFixture {
	f := &plannerFixture{
		nodes: map[string]api.DeviceTopologyNodeState{}, devices: map[api.DeviceKey]api.TopologyDevice{},
		domains: map[api.LocalDomainKey]api.DeviceDomain{}, fabrics: map[api.FabricKey]api.FabricDomain{},
		localsByClass: map[api.DomainClassKey][]api.LocalDomainKey{}, fabricsByClass: map[api.DomainClassKey][]api.FabricKey{},
	}
	for _, name := range names {
		f.nodes[name] = api.DeviceTopologyNodeState{Identity: api.NodeIdentity{Name: name, UID: types.UID("uid-" + name)}, SyncState: api.DeviceTopologyNodeSynced, SourceGeneration: 1, FreshUntil: plannerNow.Add(time.Hour)}
	}
	return f
}

func plannerClass(resourceName corev1.ResourceName, scope scheduling.DeviceTopologyDomainScope, name string) api.DomainClassKey {
	return api.DomainClassKey{ResourceName: resourceName, Scope: scope, Name: scheduling.DeviceTopologyDomainClass(name)}
}

func (f *plannerFixture) keys(node string, resourceName corev1.ResourceName, count int) []api.DeviceKey {
	keys := make([]api.DeviceKey, count)
	for index := range keys {
		key := api.DeviceKey{ResourceName: resourceName, OwnerNodeUID: f.nodes[node].Identity.UID,
			ID: api.DeviceID{ProviderID: "mock", Namespace: "example.com", Value: api.SourceDeviceID(fmt.Sprintf("%s-%s-%02d", node, resourceName, index))}}
		keys[index] = key
		f.devices[key] = api.TopologyDevice{Key: key, NodeName: node, Health: api.DeviceHealthy, SourceGeneration: 1}
	}
	return keys
}

func (f *plannerFixture) domain(node, id string, class api.DomainClassKey, keys ...api.DeviceKey) api.LocalDomainKey {
	key := api.LocalDomainKey{ResourceName: class.ResourceName, OwnerNodeUID: f.nodes[node].Identity.UID,
		ID: api.DomainID{ProviderID: "mock", Namespace: "example.com", Value: api.SourceDomainID(id)}}
	f.domains[key] = api.DeviceDomain{Key: key, Class: class, NodeName: node, MemberDeviceKeys: keys, EffectiveDeviceKeys: keys, SourceGeneration: 1}
	f.localsByClass[class] = append(f.localsByClass[class], key)
	return key
}

func (f *plannerFixture) fabric(id string, class api.DomainClassKey, domains map[string][]api.LocalDomainKey) api.FabricKey {
	key := api.FabricKey{ResourceName: class.ResourceName, ProviderID: "mock", Namespace: "example.com", Value: api.SourceFabricID(id)}
	members := make([]api.FabricMember, 0, len(domains))
	for node, keys := range domains {
		members = append(members, api.FabricMember{NodeName: node, NodeUID: f.nodes[node].Identity.UID, SourceGeneration: 1, LocalDomainKeys: keys})
	}
	// The owner is explicit and stable, independent of map iteration.
	owner := ""
	for node := range domains {
		if owner == "" || node < owner {
			owner = node
		}
	}
	f.fabrics[key] = api.FabricDomain{Key: key, Class: class, OwnerNodeUID: f.nodes[owner].Identity.UID, SourceGeneration: 1, Members: members}
	f.fabricsByClass[class] = append(f.fabricsByClass[class], key)
	return key
}

func (f *plannerFixture) snapshot() *api.DeviceTopologySnapshot {
	return api.NewDeviceTopologySnapshot(f.nodes, f.devices, f.domains, f.fabrics, f.localsByClass, f.fabricsByClass, nil)
}

func plannerTask(name string, requests map[corev1.ResourceName]int64) *api.TaskInfo {
	resources := corev1.ResourceList{}
	for name, count := range requests {
		resources[name] = *resource.NewQuantity(count, resource.DecimalSI)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{Limits: resources}}}}}
	task := api.NewTaskInfo(pod)
	task.Job = "job"
	return task
}

func plannerPolicy(class api.DomainClassKey, applyTo scheduling.DeviceTopologyApplyTo, subJob api.SubJobID) HardPlanPolicy {
	return HardPlanPolicy{Policy: api.CanonicalDeviceTopologyPolicy{ResourceName: class.ResourceName, Mode: scheduling.HardDeviceTopologyMode, ApplyTo: applyTo, DomainClass: class},
		Group: HardGroupRef{Job: "job", SubJob: subJob}}
}

func plannerInput(snapshot *api.DeviceTopologySnapshot, tasks ...HardPlanTask) HardPlanInput {
	return HardPlanInput{Snapshot: snapshot, Tasks: tasks, Now: plannerNow}
}

func TestHardPlannerPodAndGroupNodeSemantics(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	f := newPlannerFixture("node-a", "node-b")
	keysA := f.keys("node-a", plannerGPU, 4)
	keysB := f.keys("node-b", plannerGPU, 2)
	first := f.domain("node-a", "d0", local, keysA[:2]...)
	second := f.domain("node-a", "d1", local, keysA[2:]...)
	f.domain("node-b", "d0", local, keysB...)
	pod := plannerPolicy(local, scheduling.DeviceTopologyApplyToPod, "")
	group := plannerPolicy(local, scheduling.DeviceTopologyApplyToGroup, "")
	tasks := []HardPlanTask{{Task: plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 2}), NodeName: "node-a", Policies: []HardPlanPolicy{pod}},
		{Task: plannerTask("b", map[corev1.ResourceName]int64{plannerGPU: 2}), NodeName: "node-a", Policies: []HardPlanPolicy{pod}}}
	plan, failure := PlanHardGroup(plannerInput(f.snapshot(), tasks...))
	if failure != nil {
		t.Fatal(failure)
	}
	gotA := plan.Tasks[0].Resources[0].Placements[0].LocalDomain
	gotB := plan.Tasks[1].Resources[0].Placements[0].LocalDomain
	if gotA == nil || gotB == nil || *gotA == *gotB {
		t.Fatalf("Pod policies should choose distinct occupied domains: %#v", plan)
	}

	tasks[0].Task = plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1})
	tasks[1].Task = plannerTask("b", map[corev1.ResourceName]int64{plannerGPU: 1})
	tasks[0].Policies, tasks[1].Policies = []HardPlanPolicy{group}, []HardPlanPolicy{group}
	plan, failure = PlanHardGroup(plannerInput(f.snapshot(), tasks...))
	if failure != nil {
		t.Fatal(failure)
	}
	if len(plan.Anchors) != 1 || plan.Anchors[0].LocalDomain == nil || *plan.Anchors[0].LocalDomain != first {
		t.Fatalf("Group anchor = %#v, want %v", plan.Anchors, first)
	}
	if plan.Tasks[0].Resources[0].DeviceKeys[0] == plan.Tasks[1].Resources[0].DeviceKeys[0] {
		t.Fatalf("Group reused one DeviceKey: %#v", plan)
	}

	tasks[1].NodeName = "node-b"
	_, failure = PlanHardGroup(plannerInput(f.snapshot(), tasks...))
	if failure == nil || failure.Reason != hardPlanUnsatisfiableReason {
		t.Fatalf("cross-node Group+Node failure = %v", failure)
	}

	tasks[1].NodeName = "node-a"
	input := plannerInput(f.snapshot(), tasks...)
	input.Anchors = []HardGroupAnchor{{Group: HardGroupRef{Job: "job"}, Class: local, LocalDomain: &second}}
	plan, failure = PlanHardGroup(input)
	if failure != nil || len(plan.Anchors) != 1 || *plan.Anchors[0].LocalDomain != second {
		t.Fatalf("restored anchor plan = %#v, failure = %v", plan, failure)
	}
}

func TestHardPlannerFabricAndNodePolicyIntersection(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	fabricClass := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeFabric, "fabric")
	f := newPlannerFixture("node-a", "node-b")
	aKeys := f.keys("node-a", plannerGPU, 4)
	bKeys := f.keys("node-b", plannerGPU, 2)
	a0 := f.domain("node-a", "a0", local, aKeys[:2]...)
	a1 := f.domain("node-a", "a1", local, aKeys[2:]...)
	b0 := f.domain("node-b", "b0", local, bKeys...)
	common := f.fabric("common", fabricClass, map[string][]api.LocalDomainKey{"node-a": {a1}, "node-b": {b0}})
	f.fabric("a-only", fabricClass, map[string][]api.LocalDomainKey{"node-a": {a0}})
	taskA := plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 2})
	taskB := plannerTask("b", map[corev1.ResourceName]int64{plannerGPU: 2})
	nodePolicy := plannerPolicy(local, scheduling.DeviceTopologyApplyToPod, "")
	groupFabric := plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToGroup, "")
	input := plannerInput(f.snapshot(),
		HardPlanTask{Task: taskA, NodeName: "node-a", Policies: []HardPlanPolicy{groupFabric, nodePolicy}},
		HardPlanTask{Task: taskB, NodeName: "node-b", Policies: []HardPlanPolicy{groupFabric, nodePolicy}})
	plan, failure := PlanHardGroup(input)
	if failure != nil {
		t.Fatal(failure)
	}
	if len(plan.Anchors) != 1 || plan.Anchors[0].Fabric == nil || *plan.Anchors[0].Fabric != common {
		t.Fatalf("common Fabric anchor = %#v", plan.Anchors)
	}
	for _, key := range plan.Tasks[0].Resources[0].DeviceKeys {
		if key != aKeys[2] && key != aKeys[3] {
			t.Fatalf("Node+Fabric AND selected %v outside node-a member domain", key)
		}
	}
	// A later wave gets only the already bound assignment and recovered Fabric
	// anchor. It must stay on common even though a-only is also available.
	nextWave := plannerInput(f.snapshot(), HardPlanTask{Task: taskA, NodeName: "node-a", Policies: []HardPlanPolicy{groupFabric}})
	nextWave.Anchors = []HardGroupAnchor{{Group: HardGroupRef{Job: "job"}, Class: fabricClass, Fabric: &common}}
	nextWave.Occupied = bKeys
	plan, failure = PlanHardGroup(nextWave)
	if failure != nil || len(plan.Anchors) != 1 || *plan.Anchors[0].Fabric != common {
		t.Fatalf("cross-wave Fabric anchor = %#v, failure = %v", plan.Anchors, failure)
	}

	// Fabric membership is a union of local domains on this Node when no
	// separate Pod+Node policy requires one local domain.
	f2 := newPlannerFixture("node-a")
	eight := f2.keys("node-a", plannerGPU, 8)
	d0 := f2.domain("node-a", "six", local, eight[:6]...)
	d1 := f2.domain("node-a", "two", local, eight[6:]...)
	f2.fabric("all", fabricClass, map[string][]api.LocalDomainKey{"node-a": {d0, d1}})
	eightTask := plannerTask("eight", map[corev1.ResourceName]int64{plannerGPU: 8})
	_, failure = PlanHardGroup(plannerInput(f2.snapshot(), HardPlanTask{Task: eightTask, NodeName: "node-a", Policies: []HardPlanPolicy{nodePolicy}}))
	if failure == nil || failure.Reason != hardPlanUnsatisfiableReason {
		t.Fatalf("6+2 local fragmentation failure = %v", failure)
	}
	plan, failure = PlanHardGroup(plannerInput(f2.snapshot(), HardPlanTask{Task: eightTask, NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToPod, "")}}))
	if failure != nil || len(plan.Tasks[0].Resources[0].DeviceKeys) != 8 {
		t.Fatalf("Node-local Fabric union plan = %#v, failure = %v", plan, failure)
	}
}

func TestHardPlannerPodFabricCanDifferButGroupFabricCannot(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	fabricClass := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeFabric, "fabric")
	f := newPlannerFixture("node-a", "node-b")
	a := f.domain("node-a", "a", local, f.keys("node-a", plannerGPU, 1)...)
	b := f.domain("node-b", "b", local, f.keys("node-b", plannerGPU, 1)...)
	f.fabric("only-a", fabricClass, map[string][]api.LocalDomainKey{"node-a": {a}})
	f.fabric("only-b", fabricClass, map[string][]api.LocalDomainKey{"node-b": {b}})
	tasks := []HardPlanTask{{Task: plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1}), NodeName: "node-a"},
		{Task: plannerTask("b", map[corev1.ResourceName]int64{plannerGPU: 1}), NodeName: "node-b"}}
	for index := range tasks {
		tasks[index].Policies = []HardPlanPolicy{plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToPod, "")}
	}
	plan, failure := PlanHardGroup(plannerInput(f.snapshot(), tasks...))
	if failure != nil || len(plan.Tasks) != 2 {
		t.Fatalf("Pod+Fabric plan = %#v, failure = %v", plan, failure)
	}
	tasks[0].Policies = []HardPlanPolicy{plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToGroup, "")}
	tasks[1].Policies = tasks[0].Policies
	_, failure = PlanHardGroup(plannerInput(f.snapshot(), tasks...))
	if failure == nil || failure.Reason != hardPlanUnsatisfiableReason {
		t.Fatalf("Group+Fabric incompatible members failure = %v", failure)
	}
}

func TestHardPlannerMultiResourceSubGroupAndDeterminism(t *testing.T) {
	gpuLocal := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	npuLocal := plannerClass(plannerNPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	f := newPlannerFixture("node-a")
	gpu := f.keys("node-a", plannerGPU, 2)
	npu := f.keys("node-a", plannerNPU, 2)
	f.domain("node-a", "gpu", gpuLocal, gpu...)
	f.domain("node-a", "npu", npuLocal, npu...)
	task := plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1, plannerNPU: 1})
	policies := []HardPlanPolicy{plannerPolicy(npuLocal, scheduling.DeviceTopologyApplyToGroup, "sub-a"), plannerPolicy(gpuLocal, scheduling.DeviceTopologyApplyToGroup, "sub-a")}
	input := plannerInput(f.snapshot(), HardPlanTask{Task: task, NodeName: "node-a", Policies: policies})
	plan, failure := PlanHardGroup(input)
	if failure != nil {
		t.Fatal(failure)
	}
	if len(plan.Tasks[0].Resources) != 2 || len(plan.Anchors) != 2 || plan.Tasks[0].Resources[0].Request.ResourceName != plannerNPU {
		t.Fatalf("multi-resource plan = %#v", plan)
	}
	before := api.NewDeviceTopologySnapshot(input.Snapshot.Nodes, input.Snapshot.Devices, input.Snapshot.LocalDomains, input.Snapshot.Fabrics, input.Snapshot.LocalDomainsByClass, input.Snapshot.FabricsByClass, input.Snapshot.PendingFabrics)
	reversed := []HardPlanPolicy{policies[1], policies[0]}
	input.Tasks[0].Policies = reversed
	again, failure := PlanHardGroup(input)
	if failure != nil || !reflect.DeepEqual(plan, again) {
		t.Fatalf("input order changed plan: before=%#v after=%#v failure=%v", plan, again, failure)
	}
	if !reflect.DeepEqual(input.Snapshot, before) {
		t.Fatal("planner mutated snapshot")
	}
	if !reflect.DeepEqual(input.Tasks[0].Policies, reversed) {
		t.Fatal("planner mutated policies")
	}
}

func TestHardPlannerSeparateSubGroupsAndShuffledInputs(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	f := newPlannerFixture("node-a")
	keys := f.keys("node-a", plannerGPU, 3)
	d0 := f.domain("node-a", "d0", local, keys[0])
	d1 := f.domain("node-a", "d1", local, keys[1])
	d2 := f.domain("node-a", "d2", local, keys[2])
	first := HardPlanTask{Task: plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1}), NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(local, scheduling.DeviceTopologyApplyToGroup, "sub-a")}}
	second := HardPlanTask{Task: plannerTask("b", map[corev1.ResourceName]int64{plannerGPU: 1}), NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(local, scheduling.DeviceTopologyApplyToGroup, "sub-b")}}
	third := HardPlanTask{Task: plannerTask("c", map[corev1.ResourceName]int64{plannerGPU: 1}), NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(local, scheduling.DeviceTopologyApplyToGroup, "sub-c")}}
	snapshot := f.snapshot()
	input := plannerInput(snapshot, first, second, third)
	want, failure := PlanHardGroup(input)
	if failure != nil {
		t.Fatal(failure)
	}
	if len(want.Anchors) != 3 || want.Anchors[0].Group.SubJob != "sub-a" || want.Anchors[1].Group.SubJob != "sub-b" || want.Anchors[2].Group.SubJob != "sub-c" ||
		*want.Anchors[0].LocalDomain != d0 || *want.Anchors[1].LocalDomain != d1 || *want.Anchors[2].LocalDomain != d2 {
		t.Fatalf("SubGroup anchors = %#v", want.Anchors)
	}
	for seed := int64(0); seed < 24; seed++ {
		random := rand.New(rand.NewSource(seed))
		input.Tasks = []HardPlanTask{first, second, third}
		random.Shuffle(len(input.Tasks), func(i, j int) { input.Tasks[i], input.Tasks[j] = input.Tasks[j], input.Tasks[i] })
		snapshot.LocalDomainsByClass[local] = []api.LocalDomainKey{d0, d1, d2}
		random.Shuffle(3, func(i, j int) {
			snapshot.LocalDomainsByClass[local][i], snapshot.LocalDomainsByClass[local][j] = snapshot.LocalDomainsByClass[local][j], snapshot.LocalDomainsByClass[local][i]
		})
		got, failure := PlanHardGroup(input)
		if failure != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d changed plan: got=%#v, want=%#v, failure=%v", seed, got, want, failure)
		}
	}
}

func TestHardPlannerDeduplicatesOverlappingFabricDomains(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	fabricClass := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeFabric, "fabric")
	f := newPlannerFixture("node-a")
	key := f.keys("node-a", plannerGPU, 1)[0]
	d0 := f.domain("node-a", "d0", local, key)
	d1 := f.domain("node-a", "d1", local, key)
	f.fabric("overlap", fabricClass, map[string][]api.LocalDomainKey{"node-a": {d0, d1}})
	task := plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 2})
	_, failure := PlanHardGroup(plannerInput(f.snapshot(), HardPlanTask{Task: task, NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToPod, "")}}))
	if failure == nil || failure.Reason != hardPlanUnsatisfiableReason {
		t.Fatalf("overlapping domains doubled capacity: %v", failure)
	}
}

func TestHardPlannerPreservesContainerRefAcrossResources(t *testing.T) {
	gpuClass := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	npuClass := plannerClass(plannerNPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	f := newPlannerFixture("node-a")
	f.domain("node-a", "gpu", gpuClass, f.keys("node-a", plannerGPU, 1)...)
	f.domain("node-a", "npu", npuClass, f.keys("node-a", plannerNPU, 1)...)
	count := *resource.NewQuantity(1, resource.DecimalSI)
	restart := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mixed", Namespace: "default", UID: "mixed"}, Spec: corev1.PodSpec{
		Containers:     []corev1.Container{{Name: "gpu-worker", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{plannerGPU: count}}}},
		InitContainers: []corev1.Container{{Name: "npu-sidecar", RestartPolicy: &restart, Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{plannerNPU: count}}}},
	}}
	task := api.NewTaskInfo(pod)
	task.Job = "job"
	plan, failure := PlanHardGroup(plannerInput(f.snapshot(), HardPlanTask{Task: task, NodeName: "node-a", Policies: []HardPlanPolicy{
		plannerPolicy(gpuClass, scheduling.DeviceTopologyApplyToPod, ""), plannerPolicy(npuClass, scheduling.DeviceTopologyApplyToPod, ""),
	}}))
	if failure != nil {
		t.Fatal(failure)
	}
	resources := plan.Tasks[0].Resources
	if len(resources) != 2 || resources[0].Request.Container != (api.XPUContainerRef{Kind: api.XPUContainerRestartableInit, Name: "npu-sidecar"}) ||
		resources[1].Request.Container != (api.XPUContainerRef{Kind: api.XPUContainerRegular, Name: "gpu-worker"}) {
		t.Fatalf("ContainerRefs = %#v", resources)
	}
}

func TestHardPlannerBacktracksAndHonorsOccupiedKeys(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	fabricClass := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeFabric, "fabric")
	f := newPlannerFixture("node-a")
	keys := f.keys("node-a", plannerGPU, 2)
	both := f.domain("node-a", "both", local, keys...)
	onlyFirst := f.domain("node-a", "first", local, keys[0])
	f.fabric("both", fabricClass, map[string][]api.LocalDomainKey{"node-a": {both}})
	flexible := plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1})
	constrained := plannerTask("b", map[corev1.ResourceName]int64{plannerGPU: 1})
	// The class index for the constrained policy contains both domains. Its
	// anchored Group instance restricts it to the single-key domain.
	input := plannerInput(f.snapshot(),
		HardPlanTask{Task: flexible, NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToPod, "")}},
		HardPlanTask{Task: constrained, NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(local, scheduling.DeviceTopologyApplyToGroup, "sub")}})
	input.Anchors = []HardGroupAnchor{{Group: HardGroupRef{Job: "job", SubJob: "sub"}, Class: local, LocalDomain: &onlyFirst}}
	plan, failure := PlanHardGroup(input)
	if failure != nil {
		t.Fatal(failure)
	}
	if plan.Tasks[0].Resources[0].DeviceKeys[0] != keys[1] || plan.Tasks[1].Resources[0].DeviceKeys[0] != keys[0] {
		t.Fatalf("backtracking selection = %#v", plan)
	}
	input.Occupied = []api.DeviceKey{keys[0]}
	_, failure = PlanHardGroup(input)
	if failure == nil || failure.Reason != hardPlanUnsatisfiableReason {
		t.Fatalf("occupied key failure = %v", failure)
	}
	input.Occupied = nil
	input.MaxStates = 1
	_, failure = PlanHardGroup(input)
	if failure == nil || failure.Reason != hardPlanUnavailableReason {
		t.Fatalf("budget failure = %v", failure)
	}
}

func TestHardPlannerRejectsStaleNodeAndReplacementUID(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	f := newPlannerFixture("node-a")
	keys := f.keys("node-a", plannerGPU, 1)
	f.domain("node-a", "all", local, keys...)
	task := HardPlanTask{Task: plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1}), NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(local, scheduling.DeviceTopologyApplyToPod, "")}}
	snapshot := f.snapshot()
	input := plannerInput(snapshot, task)
	state := snapshot.Nodes["node-a"]
	state.FreshUntil = plannerNow
	snapshot.Nodes["node-a"] = state
	_, failure := PlanHardGroup(input)
	if failure == nil || failure.Reason != api.XPUTopologyStaleReason {
		t.Fatalf("stale fact failure = %v", failure)
	}
	state.FreshUntil = plannerNow.Add(time.Hour)
	state.Identity.UID = "replacement-uid"
	snapshot.Nodes["node-a"] = state
	_, failure = PlanHardGroup(input)
	if failure == nil || failure.Reason != hardPlanUnsatisfiableReason {
		t.Fatalf("NodeUID replacement failure = %v", failure)
	}
}

func TestHardPlannerRejectsIncompleteFabricAndUnsupportedClass(t *testing.T) {
	local := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local")
	fabricClass := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeFabric, "fabric")
	f := newPlannerFixture("node-a", "node-b")
	a := f.domain("node-a", "a", local, f.keys("node-a", plannerGPU, 1)...)
	b := f.domain("node-b", "b", local, f.keys("node-b", plannerGPU, 1)...)
	f.fabric("ab", fabricClass, map[string][]api.LocalDomainKey{"node-a": {a}, "node-b": {b}})
	state := f.nodes["node-b"]
	state.FreshUntil = plannerNow
	f.nodes["node-b"] = state
	task := plannerTask("a", map[corev1.ResourceName]int64{plannerGPU: 1})
	input := plannerInput(f.snapshot(), HardPlanTask{Task: task, NodeName: "node-a", Policies: []HardPlanPolicy{plannerPolicy(fabricClass, scheduling.DeviceTopologyApplyToPod, "")}})
	_, failure := PlanHardGroup(input)
	if failure == nil || failure.Reason != api.XPUTopologyDataNotReadyReason {
		t.Fatalf("stale Fabric member failure = %v", failure)
	}
	input.Snapshot.ProviderCapabilitiesKnown = true
	input.Snapshot.SupportedProviderDomainClasses = map[api.DomainClassKey]struct{}{}
	_, failure = PlanHardGroup(input)
	if failure == nil || failure.Reason != api.XPUTopologyDomainClassUnsupportedReason {
		t.Fatalf("unsupported class failure = %v", failure)
	}
}
