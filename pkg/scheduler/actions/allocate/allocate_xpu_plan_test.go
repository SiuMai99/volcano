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

package allocate

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/binpack"
	"volcano.sh/volcano/pkg/scheduler/plugins/drf"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/nodeorder"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/proportion"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestXPUHardTrialRetriesAnotherOrdinaryNodeAndReplansWinner(t *testing.T) {
	gpu := corev1.ResourceName("nvidia.com/gpu")
	quantities := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), gpu: resource.MustParse("1")}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "default", UID: "worker"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{Limits: quantities}}}}}
	pod.Status.NominatedNodeName = "node-a"
	task := api.NewTaskInfo(pod)
	job := api.NewJobInfo("default/train", task)
	task.Job = job.UID
	job.MinAvailable = 1
	job.Queue = "q"
	job.DeviceTopology = api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
		ResourceName: gpu, Mode: scheduling.HardDeviceTopologyMode, ApplyTo: scheduling.DeviceTopologyApplyToPod,
	}}}
	capacity := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), gpu: resource.MustParse("2"), corev1.ResourcePods: resource.MustParse("10")}
	nodes := map[string]*api.NodeInfo{}
	for _, name := range []string{"node-a", "node-b"} {
		nodes[name] = api.NewNodeInfo(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
			Status: corev1.NodeStatus{Allocatable: capacity, Capacity: capacity}})
	}
	ssn := &framework.Session{Jobs: map[api.JobID]*api.JobInfo{job.UID: job}, Nodes: nodes,
		Queues:        map[api.QueueID]*api.QueueInfo{"q": {UID: "q", Name: "q"}},
		RealNodesList: map[string][]*api.NodeInfo{framework.ClusterTopHyperNode: {nodes["node-a"], nodes["node-b"]}},
		NodesInShard:  sets.New("node-a", "node-b")}
	var calls []string
	firstNode := ""
	rejectWinner := false
	if err := ssn.AddXPUHardPlanFn(func(view []framework.AllocationPlacement) (framework.XPUHardPlan, error) {
		if len(view) != 1 {
			return framework.XPUHardPlan{}, fmt.Errorf("expected one Allocate, got %#v", view)
		}
		calls = append(calls, view[0].NodeName)
		if rejectWinner {
			return framework.XPUHardPlan{}, fmt.Errorf("XPUTopologyDataNotReady: snapshot changed before final plan")
		}
		if firstNode == "" {
			firstNode = view[0].NodeName
			return framework.XPUHardPlan{}, fmt.Errorf("XPUTopologyUnsatisfiable: no local domain")
		}
		return framework.XPUHardPlan{Tasks: []framework.XPUPlannedTask{{
			TaskID: view[0].TaskID, PodUID: view[0].PodUID, NodeName: view[0].NodeName,
		}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	queue := util.NewPriorityQueue(nil)
	queue.Push(task)
	alloc := New()
	alloc.session = ssn
	stmt := alloc.allocateResourcesForTasks(job.SubJobs[job.DefaultSubJobID()], queue, framework.ClusterTopHyperNode)
	if stmt == nil {
		t.Fatalf("hard trial found no alternative ordinary Node; planner calls = %v", calls)
	}
	view := stmt.AllocationView()
	if len(view) != 1 || view[0].NodeName != "node-b" || len(calls) != 2 || calls[0] != "node-a" || calls[1] != "node-b" {
		t.Fatalf("trial view = %#v, planner calls = %v", view, calls)
	}
	// The final call must use the winning Statement view again. There is no
	// cache in this fixture, so an accidental Bind enqueue would panic.
	finalPlan, err := alloc.finishXPUHardTrial(job, stmt)
	if err != nil || len(finalPlan.Tasks) != 1 || finalPlan.Tasks[0].NodeName != view[0].NodeName ||
		len(calls) != 3 || calls[2] != view[0].NodeName || task.Status != api.Pending {
		t.Fatalf("winner was not replanned and discarded: plan=%#v err=%v calls=%v status=%s", finalPlan, err, calls, task.Status)
	}
	if pod.Annotations[api.XPUAssignmentAnnotationKey] != "" {
		t.Fatal("trial wrote API assignment metadata")
	}
	rejectWinner = true
	stale := framework.NewXPUHardTrialStatement(ssn)
	if err := stale.Allocate(task, nodes["node-b"]); err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.finishXPUHardTrial(job, stale); err == nil || task.Status != api.Pending || pod.Spec.NodeName != "" {
		t.Fatalf("stale winning plan was not rejected and discarded: err=%v status=%s node=%q", err, task.Status, pod.Spec.NodeName)
	}
}

func TestAllocateExecuteHardXPUPlansButNeverBinds(t *testing.T) {
	gpu := corev1.ResourceName("nvidia.com/gpu")
	pg := util.BuildPodGroup("train", "default", "default", 1, nil, scheduling.PodGroupInqueue)
	pg.Spec.DeviceTopology = &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{{
		ResourceName: gpu, Mode: scheduling.HardDeviceTopologyMode, ApplyTo: scheduling.DeviceTopologyApplyToPod,
		Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
	}}}
	resources := api.BuildResourceList("1", "1G", api.ScalarResource{Name: string(gpu), Value: "1"})
	test := uthelper.TestCommonStruct{
		Name: "hard xPU Statement trial remains non-binding",
		Plugins: map[string]framework.PluginBuilder{
			binpack.PluginName: binpack.New, drf.PluginName: drf.New, gang.PluginName: gang.New,
			nodeorder.PluginName: nodeorder.New, predicates.PluginName: predicates.New, proportion.PluginName: proportion.New,
		},
		PodGroups: []*scheduling.PodGroup{pg},
		Pods:      []*corev1.Pod{util.BuildPod("default", "worker", "", corev1.PodPending, resources, "train", nil, nil)},
		Nodes: []*corev1.Node{util.BuildNode("node-a", api.BuildResourceList("4", "8Gi",
			api.ScalarResource{Name: string(gpu), Value: "2"}, api.ScalarResource{Name: "pods", Value: "10"}), nil)},
		Queues:        []*scheduling.Queue{util.BuildQueue("default", 1, nil)},
		ExpectBindMap: map[string]string{}, ExpectBindsNum: 0,
	}
	enabled := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: gang.PluginName, EnabledJobReady: &enabled, EnabledJobPipelined: &enabled, EnabledJobOrder: &enabled},
		{Name: drf.PluginName, EnabledJobOrder: &enabled},
		{Name: proportion.PluginName, EnabledQueueOrder: &enabled, EnabledAllocatable: &enabled},
		{Name: predicates.PluginName, EnabledPredicate: &enabled},
		{Name: nodeorder.PluginName, EnabledNodeOrder: &enabled},
	}}}
	ssn := test.RegisterSession(tiers, nil)
	defer test.Close()
	job := ssn.Jobs[api.JobID("default/train")]
	if job == nil || !job.HasHardDeviceTopologyPolicy() {
		t.Fatalf("hard policy missing from Session Job: %#v", job)
	}
	calls := 0
	if err := ssn.AddXPUHardPlanFn(func(view []framework.AllocationPlacement) (framework.XPUHardPlan, error) {
		calls++
		if len(view) != 1 || view[0].NodeName != "node-a" {
			return framework.XPUHardPlan{}, fmt.Errorf("incomplete admission wave: %#v", view)
		}
		return framework.XPUHardPlan{Tasks: []framework.XPUPlannedTask{{
			TaskID: view[0].TaskID, PodUID: view[0].PodUID, NodeName: view[0].NodeName,
		}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	test.Run([]framework.Action{New()})
	if calls != 2 {
		t.Fatalf("planner called %d times; want trial and winning replan", calls)
	}
	for _, task := range job.Tasks {
		if task.Status != api.Pending || task.Pod.Spec.NodeName != "" {
			t.Fatalf("trial leaked Task status %s or Pod node %q", task.Status, task.Pod.Spec.NodeName)
		}
	}
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

func TestAllocateHardXPURevisitsEarlierSubGroupNode(t *testing.T) {
	gpu := corev1.ResourceName("nvidia.com/gpu")
	pg := util.BuildPodGroupWithSubGroupPolicy("train", "default", "", "default", 2, nil, scheduling.PodGroupInqueue, "", 0,
		[]scheduling.SubGroupPolicySpec{util.BuildSubGroupPolicy("roles", []string{"volcano.sh/task-spec"}, "", 0)})
	pg.Spec.DeviceTopology = &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{{
		ResourceName: gpu, Mode: scheduling.HardDeviceTopologyMode, ApplyTo: scheduling.DeviceTopologyApplyToGroup,
		Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
	}}}
	resources := api.BuildResourceList("1", "1G", api.ScalarResource{Name: string(gpu), Value: "1"})
	pods := []*corev1.Pod{
		util.BuildPod("default", "a", "", corev1.PodPending, resources, "train", map[string]string{"volcano.sh/task-spec": "a"}, nil),
		util.BuildPod("default", "b", "", corev1.PodPending, resources, "train", map[string]string{"volcano.sh/task-spec": "b"}, nil),
	}
	pods[0].Status.NominatedNodeName = "node-a"
	pods[1].Status.NominatedNodeName = "node-a"
	capacity := api.BuildResourceList("4", "8Gi", api.ScalarResource{Name: string(gpu), Value: "2"}, api.ScalarResource{Name: "pods", Value: "10"})
	test := uthelper.TestCommonStruct{
		Name: "hard xPU Group explores an earlier SubGroup Node again",
		Plugins: map[string]framework.PluginBuilder{
			binpack.PluginName: binpack.New, drf.PluginName: drf.New, gang.PluginName: gang.New,
			nodeorder.PluginName: nodeorder.New, predicates.PluginName: predicates.New, proportion.PluginName: proportion.New,
		},
		PodGroups: []*scheduling.PodGroup{pg}, Pods: pods,
		Nodes:         []*corev1.Node{util.BuildNode("node-a", capacity, nil), util.BuildNode("node-b", capacity, nil)},
		Queues:        []*scheduling.Queue{util.BuildQueue("default", 1, nil)},
		ExpectBindMap: map[string]string{}, ExpectBindsNum: 0,
	}
	enabled := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: gang.PluginName, EnabledJobReady: &enabled, EnabledJobPipelined: &enabled, EnabledSubJobReady: &enabled, EnabledSubJobOrder: &enabled},
		{Name: drf.PluginName, EnabledJobOrder: &enabled},
		{Name: proportion.PluginName, EnabledQueueOrder: &enabled, EnabledAllocatable: &enabled},
		{Name: predicates.PluginName, EnabledPredicate: &enabled},
		{Name: nodeorder.PluginName, EnabledNodeOrder: &enabled},
	}}}
	ssn := test.RegisterSession(tiers, nil)
	defer test.Close()
	job := ssn.Jobs[api.JobID("default/train")]
	if job == nil || len(job.SubJobs) != 2 {
		t.Fatalf("expected two SubGroups, got %#v", job)
	}
	firstTask := api.TaskID("")
	combinedRejected := false
	winning := false
	if err := ssn.AddXPUHardPlanFn(func(view []framework.AllocationPlacement) (framework.XPUHardPlan, error) {
		if len(view) == 1 && firstTask == "" {
			firstTask = view[0].TaskID
		}
		if len(view) == 2 {
			if view[0].NodeName == "" || view[1].NodeName == "" {
				return framework.XPUHardPlan{}, fmt.Errorf("incomplete Group admission: %#v", view)
			}
			for _, placement := range view {
				if placement.TaskID == firstTask && placement.NodeName == "node-a" {
					combinedRejected = true
					return framework.XPUHardPlan{}, fmt.Errorf("XPUTopologyUnsatisfiable: first SubGroup must use node-b")
				}
				if placement.TaskID == firstTask && placement.NodeName == "node-b" {
					winning = true
				}
			}
		}
		return framework.XPUHardPlan{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	test.Run([]framework.Action{New()})
	if !combinedRejected || !winning {
		t.Fatalf("Group trial did not revisit the first SubGroup: first=%s rejected=%t winning=%t", firstTask, combinedRejected, winning)
	}
	for _, task := range job.Tasks {
		if task.Status != api.Pending || task.Pod.Spec.NodeName != "" {
			t.Fatalf("Group trial leaked Task status %s or Pod node %q", task.Status, task.Pod.Spec.NodeName)
		}
	}
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}
