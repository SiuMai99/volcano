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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func TestStatementPlannerUsesFixedNodeAndRejectsStaleIdentity(t *testing.T) {
	class := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local-scale-up")
	fixture := newPlannerFixture("node-a")
	keys := fixture.keys("node-a", plannerGPU, 2)
	fixture.domain("node-a", "d0", class, keys...)
	job, task := compilerTestJob("train", scheduling.HardDeviceTopologyMode, "local-scale-up", 2)
	task.NodeName = "node-a"
	node := api.NewNodeInfo(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: fixture.nodes["node-a"].Identity.UID}})
	snapshot := fixture.snapshot()
	ssn := &framework.Session{Jobs: map[api.JobID]*api.JobInfo{job.UID: job},
		Nodes: map[string]*api.NodeInfo{"node-a": node}, DeviceTopology: snapshot}
	planner := &sessionHardPlanner{ssn: ssn, compiler: newCompiler(compilerTestManager(t), snapshot, func() time.Time { return plannerNow }),
		now: func() time.Time { return plannerNow }}
	view := []framework.AllocationPlacement{{JobID: job.UID, TaskID: task.UID, PodUID: task.Pod.UID, NodeName: "node-a"}}
	got, err := planner.plan(view)
	if err != nil {
		t.Fatalf("plan() error = %v", err)
	}
	if len(got.Tasks) != 1 || len(got.Tasks[0].Resources) != 1 || len(got.Tasks[0].Resources[0].DeviceKeys) != 2 {
		t.Fatalf("plan() = %#v", got)
	}
	if got.Tasks[0].Resources[0].Request.Container.Name != "worker" {
		t.Fatalf("lost ContainerRef: %#v", got.Tasks[0].Resources[0].Request)
	}
	for _, key := range got.Tasks[0].Resources[0].DeviceKeys {
		if key.OwnerNodeUID != node.Node.UID {
			t.Fatalf("selected key has wrong NodeUID: %#v", key)
		}
	}
	job.DeviceTopology.Policies[0].ApplyTo = scheduling.DeviceTopologyApplyToGroup
	planner.compiler = newCompiler(compilerTestManager(t), snapshot, func() time.Time { return plannerNow })
	groupPlan, err := planner.plan(view)
	if err != nil || len(groupPlan.Anchors) != 1 || len(groupPlan.Tasks[0].Resources[0].Placements) != 1 ||
		groupPlan.Anchors[0].Group.Job != job.UID {
		t.Fatalf("Group policy plan lost selected anchor or placement: plan=%#v err=%v", groupPlan, err)
	}
	groupPlan.Anchors[0].LocalDomain.OwnerNodeUID = "mutated"
	groupPlan.Tasks[0].Resources[0].DeviceKeys[0].OwnerNodeUID = "mutated"
	again, err := planner.plan(view)
	if err != nil || again.Anchors[0].LocalDomain.OwnerNodeUID != node.Node.UID ||
		again.Tasks[0].Resources[0].DeviceKeys[0].OwnerNodeUID != node.Node.UID {
		t.Fatalf("caller mutated planner output or cached snapshot: plan=%#v err=%v", again, err)
	}

	node.Node.UID = "replacement-uid"
	if _, err := planner.plan(view); err == nil || !strings.Contains(err.Error(), api.XPUTopologyDataNotReadyReason) {
		t.Fatalf("Node replacement error = %v", err)
	}
	node.Node.UID = fixture.nodes["node-a"].Identity.UID
	view[0].PodUID = "stale-pod"
	if _, err := planner.plan(view); err == nil || !strings.Contains(err.Error(), api.XPUTopologyDataNotReadyReason) {
		t.Fatalf("stale Pod error = %v", err)
	}
	view[0].PodUID = task.Pod.UID
	ssn.DeviceTopology = fixture.snapshot()
	if _, err := planner.plan(view); err == nil || !strings.Contains(err.Error(), "Session topology view changed") {
		t.Fatalf("replaced paired snapshot error = %v", err)
	}
}

func TestStatementPlannerDoesNotAssumeAnEmptyAnchorForBoundMembers(t *testing.T) {
	class := plannerClass(plannerGPU, scheduling.DeviceTopologyDomainScopeNode, "local-scale-up")
	fixture := newPlannerFixture("node-a")
	fixture.domain("node-a", "d0", class, fixture.keys("node-a", plannerGPU, 2)...)
	job, task := compilerTestJob("train", scheduling.HardDeviceTopologyMode, "local-scale-up", 1)
	task.NodeName = "node-a"
	bound := compilerTestTask("already-bound", 1)
	bound.Job = job.UID
	bound.NodeName = "node-a"
	bound.Status = api.Bound
	job.AddTaskInfo(bound)
	node := api.NewNodeInfo(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: fixture.nodes["node-a"].Identity.UID}})
	snapshot := fixture.snapshot()
	ssn := &framework.Session{Jobs: map[api.JobID]*api.JobInfo{job.UID: job},
		Nodes: map[string]*api.NodeInfo{"node-a": node}, DeviceTopology: snapshot}
	planner := &sessionHardPlanner{ssn: ssn, compiler: newCompiler(compilerTestManager(t), snapshot, func() time.Time { return plannerNow }),
		now: func() time.Time { return plannerNow }}
	view := []framework.AllocationPlacement{{JobID: job.UID, TaskID: task.UID, PodUID: task.Pod.UID, NodeName: "node-a"}}
	if _, err := planner.plan(view); err == nil || !strings.Contains(err.Error(), "requires Pod-derived anchor recovery") {
		t.Fatalf("missing bound anchor error = %v", err)
	}
	bound.Status = api.Allocated
	if _, err := planner.plan(view); err != nil {
		t.Fatalf("another SubGroup's speculative Allocate was treated as bound: %v", err)
	}
}
