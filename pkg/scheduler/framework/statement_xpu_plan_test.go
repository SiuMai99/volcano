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

package framework

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

func TestStatementAllocationViewIsDetachedAndSorted(t *testing.T) {
	makeTask := func(id string) *api.TaskInfo {
		task := &api.TaskInfo{UID: api.TaskID(id), Job: "job",
			Pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("pod-" + id)}}}
		task.NodeName = "node-" + id
		return task
	}
	a, b := makeTask("a"), makeTask("b")
	stmt := &Statement{operations: []operation{{name: Pipeline, task: a}, {name: Allocate, task: b}, {name: Allocate, task: a}}}
	want := []AllocationPlacement{{JobID: "job", TaskID: "a", PodUID: "pod-a", NodeName: "node-a"},
		{JobID: "job", TaskID: "b", PodUID: "pod-b", NodeName: "node-b"}}
	view := stmt.AllocationView()
	if !reflect.DeepEqual(view, want) {
		t.Fatalf("AllocationView() = %#v, want %#v", view, want)
	}
	view[0].NodeName = "changed"
	if got := stmt.AllocationView(); !reflect.DeepEqual(got, want) {
		t.Fatalf("caller changed Statement allocation view: %#v", got)
	}
}

func TestXPUHardTrialSaveRecoverCannotCommitToBind(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "default", UID: "pod-worker"}}
	task := api.NewTaskInfo(pod)
	job := api.NewJobInfo("default/train", task)
	task.Job = job.UID
	job.DeviceTopology = api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
		ResourceName: "nvidia.com/gpu", Mode: scheduling.HardDeviceTopologyMode,
	}}}
	node := api.NewNodeInfo(nil)
	node.Name = "node-a"
	node.Idle = (&api.Resource{MilliCPU: 1000}).Clone()
	node.Releasing = api.EmptyResource()
	node.Pipelined = api.EmptyResource()
	ssn := &Session{Jobs: map[api.JobID]*api.JobInfo{job.UID: job}, Nodes: map[string]*api.NodeInfo{node.Name: node}}

	trial := NewXPUHardTrialStatement(ssn)
	if err := trial.Allocate(task, node); err != nil {
		t.Fatalf("hard trial Allocate() error = %v", err)
	}
	first := trial.AllocationView()
	if len(first) != 1 || first[0].NodeName != node.Name {
		t.Fatalf("trial view = %#v", first)
	}
	saved := SaveOperations(trial)
	trial.Discard()
	if len(trial.AllocationView()) != 0 {
		t.Fatal("discarded trial retained an Allocate operation")
	}
	if pod.Spec.NodeName != "" {
		t.Fatalf("discarded trial left Pod.spec.nodeName = %q", pod.Spec.NodeName)
	}
	recovered := NewXPUHardTrialStatement(ssn)
	if err := recovered.RecoverOperations(saved); err != nil {
		t.Fatalf("RecoverOperations() error = %v", err)
	}
	if got := recovered.AllocationView(); !reflect.DeepEqual(got, first) {
		t.Fatalf("recovered view = %#v, want %#v", got, first)
	}
	// There is deliberately no cache in this Session. Reaching AddBindTask
	// would panic; Commit must instead discard the speculative operations.
	recovered.Commit()
	if task.Status != api.Pending || len(recovered.AllocationView()) != 0 || pod.Spec.NodeName != "" {
		t.Fatalf("hard trial Commit left Task status %s, Pod node %q or operations %#v", task.Status, pod.Spec.NodeName, recovered.AllocationView())
	}
	if pod.Annotations[api.XPUAssignmentAnnotationKey] != "" {
		t.Fatal("trial wrote an assignment annotation")
	}
}

func TestXPUHardTrialStillRunsOtherJobValidators(t *testing.T) {
	job := &api.JobInfo{DeviceTopology: api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
		ResourceName: "nvidia.com/gpu", Mode: scheduling.HardDeviceTopologyMode,
	}}}}
	ssn := &Session{Tiers: []conf.Tier{{Plugins: []conf.PluginOption{
		{Name: xpuTopologyPluginName}, {Name: "other"},
	}}}, jobValidFns: map[string]api.ValidateExFn{}}
	ssn.AddJobValidFn(xpuTopologyPluginName, func(interface{}) *api.ValidateResult {
		return &api.ValidateResult{Reason: string(topology.AssignmentNotEnforceable)}
	})
	ssn.AddJobValidFn("other", func(interface{}) *api.ValidateResult {
		return &api.ValidateResult{Reason: "OtherPolicyRejected"}
	})
	if err := ssn.AddXPUHardPlanFn(func([]AllocationPlacement) (XPUHardPlan, error) { return XPUHardPlan{}, nil }); err != nil {
		t.Fatal(err)
	}
	if got := ssn.JobValidForXPUHardTrial(job); got == nil || got.Reason != "OtherPolicyRejected" {
		t.Fatalf("other Job validator was bypassed: %#v", got)
	}
	ssn.jobValidFns["other"] = func(interface{}) *api.ValidateResult {
		return &api.ValidateResult{Reason: string(topology.AssignmentNotEnforceable)}
	}
	if got := ssn.JobValidForXPUHardTrial(job); got == nil || got.Reason != string(topology.AssignmentNotEnforceable) {
		t.Fatalf("another plugin's assignment blocker was bypassed: %#v", got)
	}
}
