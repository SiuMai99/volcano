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

package api

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestMergePodGroupConditionsPreservesXPUTopologyBlocker(t *testing.T) {
	blocker := scheduling.PodGroupCondition{
		Type:   scheduling.PodGroupUnschedulableType,
		Status: corev1.ConditionTrue,
		Reason: XPUTopologyPolicyConflictReason,
	}
	dynamic := scheduling.PodGroupCondition{
		Type:   scheduling.PodGroupUnschedulableType,
		Status: corev1.ConditionTrue,
		Reason: "NotEnoughResources",
	}

	merged := MergePodGroupConditions([]scheduling.PodGroupCondition{blocker}, []scheduling.PodGroupCondition{dynamic})
	if len(merged) != 1 || merged[0].Reason != XPUTopologyPolicyConflictReason {
		t.Fatalf("dynamic result overwrote xPU authoring blocker: %#v", merged)
	}

	resolved := scheduling.PodGroupCondition{
		Type:   scheduling.PodGroupUnschedulableType,
		Status: corev1.ConditionFalse,
		Reason: XPUTopologyResolvedReason,
	}
	merged = MergePodGroupConditions(merged, []scheduling.PodGroupCondition{resolved})
	if len(merged) != 1 || merged[0].Reason != XPUTopologyResolvedReason || merged[0].Status != corev1.ConditionFalse {
		t.Fatalf("authoring owner could not resolve blocker: %#v", merged)
	}
}

func TestMergePodGroupStatusV1beta1PreservesOtherConditions(t *testing.T) {
	current := schedulingv1beta1.PodGroupStatus{Conditions: []schedulingv1beta1.PodGroupCondition{
		{Type: schedulingv1beta1.PodGroupScheduled, Status: corev1.ConditionTrue, Reason: "AlreadyScheduled"},
		{Type: schedulingv1beta1.PodGroupUnschedulableType, Status: corev1.ConditionTrue, Reason: XPUTopologyPolicyConflictReason},
	}}
	desired := schedulingv1beta1.PodGroupStatus{
		Running: 2,
		Conditions: []schedulingv1beta1.PodGroupCondition{
			{Type: schedulingv1beta1.PodGroupUnschedulableType, Status: corev1.ConditionTrue, Reason: "NotEnoughResources"},
		},
	}

	merged := MergePodGroupStatusV1beta1(current, desired)
	if merged.Running != 2 {
		t.Fatalf("scheduler counters were not retained: %#v", merged)
	}
	if len(merged.Conditions) != 2 {
		t.Fatalf("conditions = %#v, want scheduled condition plus authoring blocker", merged.Conditions)
	}
	if merged.Conditions[0].Reason != "AlreadyScheduled" || merged.Conditions[1].Reason != XPUTopologyPolicyConflictReason {
		t.Fatalf("status merge changed condition owners: %#v", merged.Conditions)
	}
}
