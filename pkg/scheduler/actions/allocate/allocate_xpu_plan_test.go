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

func hardXPUTestSession(t *testing.T, groupAcrossSubJobs bool) (*uthelper.TestCommonStruct, *framework.Session, *api.JobInfo) {
	t.Helper()
	gpu := corev1.ResourceName("nvidia.com/gpu")
	minMember := int32(1)
	pg := util.BuildPodGroup("train", "default", "default", minMember, nil, scheduling.PodGroupInqueue)
	applyTo := scheduling.DeviceTopologyApplyToPod
	resources := api.BuildResourceList("1", "1G", api.ScalarResource{Name: string(gpu), Value: "1"})
	pods := []*corev1.Pod{util.BuildPod("default", "worker", "", corev1.PodPending, resources, "train", nil, nil)}
	if groupAcrossSubJobs {
		pg = util.BuildPodGroupWithSubGroupPolicy("train", "default", "", "default", 2, nil, scheduling.PodGroupInqueue, "", 0,
			[]scheduling.SubGroupPolicySpec{util.BuildSubGroupPolicy("roles", []string{"volcano.sh/task-spec"}, "", 0)})
		applyTo = scheduling.DeviceTopologyApplyToGroup
		pods = []*corev1.Pod{
			util.BuildPod("default", "a", "", corev1.PodPending, resources, "train", map[string]string{"volcano.sh/task-spec": "a"}, nil),
			util.BuildPod("default", "b", "", corev1.PodPending, resources, "train", map[string]string{"volcano.sh/task-spec": "b"}, nil),
		}
	}
	pg.Spec.DeviceTopology = &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{{
		ResourceName: gpu, Mode: scheduling.HardDeviceTopologyMode, ApplyTo: applyTo,
		Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
	}}}
	test := &uthelper.TestCommonStruct{
		Name: "hard xPU Statement plans without binding",
		Plugins: map[string]framework.PluginBuilder{
			binpack.PluginName: binpack.New, drf.PluginName: drf.New, gang.PluginName: gang.New,
			nodeorder.PluginName: nodeorder.New, predicates.PluginName: predicates.New, proportion.PluginName: proportion.New,
		},
		PodGroups: []*scheduling.PodGroup{pg}, Pods: pods,
		Nodes: []*corev1.Node{util.BuildNode("node-a", api.BuildResourceList("4", "8Gi",
			api.ScalarResource{Name: string(gpu), Value: "2"}, api.ScalarResource{Name: "pods", Value: "10"}), nil)},
		Queues:        []*scheduling.Queue{util.BuildQueue("default", 1, nil)},
		ExpectBindMap: map[string]string{}, ExpectBindsNum: 0,
	}
	enabled := true
	tiers := []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: gang.PluginName, EnabledJobReady: &enabled, EnabledJobPipelined: &enabled, EnabledJobOrder: &enabled,
			EnabledSubJobReady: &enabled, EnabledSubJobOrder: &enabled},
		{Name: drf.PluginName, EnabledJobOrder: &enabled},
		{Name: proportion.PluginName, EnabledQueueOrder: &enabled, EnabledAllocatable: &enabled},
		{Name: predicates.PluginName, EnabledPredicate: &enabled},
		{Name: nodeorder.PluginName, EnabledNodeOrder: &enabled},
	}}}
	ssn := test.RegisterSession(tiers, nil)
	t.Cleanup(test.Close)
	job := ssn.Jobs[api.JobID("default/train")]
	if job == nil || !job.HasHardDeviceTopologyPolicy() {
		t.Fatalf("hard policy missing from Session Job: %#v", job)
	}
	return test, ssn, job
}

func checkHardXPUTrialDiscarded(t *testing.T, test *uthelper.TestCommonStruct, job *api.JobInfo) {
	t.Helper()
	for _, task := range job.Tasks {
		if task.Status != api.Pending || task.Pod.Spec.NodeName != "" || task.Pod.Annotations[api.XPUAssignmentAnnotationKey] != "" {
			t.Fatalf("trial leaked Task status %s, Pod node %q or assignment", task.Status, task.Pod.Spec.NodeName)
		}
	}
	if err := test.CheckAll(0); err != nil {
		t.Fatal(err)
	}
}

func TestAllocateExecuteHardXPUPlansButNeverBinds(t *testing.T) {
	test, ssn, job := hardXPUTestSession(t, false)
	calls := 0
	rejectOnCall := 0
	if err := ssn.AddXPUHardPlanFn(func(view []framework.AllocationPlacement) error {
		calls++
		if len(view) != 1 || view[0].NodeName != "node-a" {
			return fmt.Errorf("incomplete admission wave: %#v", view)
		}
		if calls == rejectOnCall {
			return fmt.Errorf("XPUTopologyUnavailable: trial or final facts changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	test.Run([]framework.Action{New()})
	if calls != 2 {
		t.Fatalf("planner called %d times; want trial and winning replan", calls)
	}
	// A failed greedy Node plan does not launch a second Node search in PR11.
	rejectOnCall = 3
	test.Run([]framework.Action{New()})
	if calls != 3 {
		t.Fatalf("failed trial called planner %d times; want one new trial", calls)
	}
	// Final revalidation can also fail after the trial passed.
	rejectOnCall = 5
	test.Run([]framework.Action{New()})
	if calls != 5 {
		t.Fatalf("stale winner called planner %d times; want trial and final replan", calls)
	}
	checkHardXPUTrialDiscarded(t, test, job)
}

func TestAllocateHardXPUPlansCompleteSubGroupAdmission(t *testing.T) {
	test, ssn, job := hardXPUTestSession(t, true)
	if len(job.SubJobs) != 2 {
		t.Fatalf("expected two SubGroups, got %d", len(job.SubJobs))
	}
	calls := 0
	reject := false
	if err := ssn.AddXPUHardPlanFn(func(view []framework.AllocationPlacement) error {
		calls++
		if len(view) != 2 || view[0].TaskID == view[1].TaskID {
			return fmt.Errorf("incomplete Group admission: %#v", view)
		}
		if reject {
			return fmt.Errorf("XPUTopologyUnsatisfiable: complete Group placement rejected")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	test.Run([]framework.Action{New()})
	if calls != 2 {
		t.Fatalf("planner called %d times; want complete trial and winning replan", calls)
	}
	reject = true
	test.Run([]framework.Action{New()})
	if calls != 3 {
		t.Fatalf("rejected Group trial called planner %d times; want one complete trial", calls)
	}
	checkHardXPUTrialDiscarded(t, test, job)
}

func TestAllocateHardXPUNominationReplansWithoutBind(t *testing.T) {
	test, ssn, job := hardXPUTestSession(t, false)
	subJob := job.SubJobs[job.DefaultSubJobID()]
	subJob.NominatedHyperNode = framework.ClusterTopHyperNode
	for _, task := range job.Tasks {
		task.Pod.Status.NominatedNodeName = "node-a"
	}
	calls := 0
	if err := ssn.AddXPUHardPlanFn(func(view []framework.AllocationPlacement) error {
		calls++
		if len(view) != 1 || view[0].NodeName != "node-a" {
			return fmt.Errorf("nomination lost its fixed Node: %#v", view)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	test.Run([]framework.Action{New()})
	if calls != 2 {
		t.Fatalf("nominated planner called %d times; want trial and winning replan", calls)
	}
	checkHardXPUTrialDiscarded(t, test, job)
}
