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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"
)

func TestSessionKeepsXPUTopologyBlockerUnschedulable(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "worker-0", UID: types.UID("worker-0")},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	job := schedulingapi.NewJobInfo("default/train", schedulingapi.NewTaskInfo(pod))
	job.SetPodGroup(&schedulingapi.PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "train"},
		Spec:       scheduling.PodGroupSpec{MinMember: 2},
		Status: scheduling.PodGroupStatus{Conditions: []scheduling.PodGroupCondition{{
			Type:   scheduling.PodGroupUnschedulableType,
			Status: corev1.ConditionTrue,
			Reason: schedulingapi.XPUTopologyPolicyConflictReason,
		}}},
	}})
	ssn := &Session{
		UID:  types.UID("scheduler-cycle"),
		Jobs: map[schedulingapi.JobID]*schedulingapi.JobInfo{job.UID: job},
	}

	status := jobStatus(ssn, job)
	if status.Phase != scheduling.PodGroupUnknown {
		t.Fatalf("phase = %q, want %q while authoring blocker is unresolved", status.Phase, scheduling.PodGroupUnknown)
	}

	err := ssn.UpdatePodGroupCondition(job, &scheduling.PodGroupCondition{
		Type:   scheduling.PodGroupUnschedulableType,
		Status: corev1.ConditionTrue,
		Reason: "NotEnoughResources",
	})
	if err != nil {
		t.Fatalf("UpdatePodGroupCondition() error = %v", err)
	}
	conditions := job.PodGroup.Status.Conditions
	if len(conditions) != 1 || conditions[0].Reason != schedulingapi.XPUTopologyPolicyConflictReason {
		t.Fatalf("dynamic scheduler reason overwrote xPU authoring blocker: %#v", conditions)
	}
}
