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

package cache

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubetesting "k8s.io/client-go/testing"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	vcv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcclientfake "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"
)

func TestDefaultStatusUpdaterRetriesAndPreservesXPUTopologyBlocker(t *testing.T) {
	current := &vcv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "train"},
		Status: vcv1beta1.PodGroupStatus{Conditions: []vcv1beta1.PodGroupCondition{
			{
				Type:   vcv1beta1.PodGroupUnschedulableType,
				Status: corev1.ConditionTrue,
				Reason: schedulingapi.XPUTopologyPolicyConflictReason,
			},
		}},
	}
	client := vcclientfake.NewSimpleClientset(current)
	statusUpdates := 0
	client.PrependReactor("update", "podgroups", func(action kubetesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		statusUpdates++
		if statusUpdates == 1 {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: vcv1beta1.SchemeGroupVersion.Group, Resource: "podgroups"},
				"train",
				errors.New("concurrent authoring status write"),
			)
		}
		return false, nil, nil
	})

	updater := &defaultStatusUpdater{vcclient: client}
	desired := &schedulingapi.PodGroup{
		Version: schedulingapi.PodGroupVersionV1Beta1,
		PodGroup: scheduling.PodGroup{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "train"},
			Status: scheduling.PodGroupStatus{
				Running: 2,
				Conditions: []scheduling.PodGroupCondition{{
					Type:   scheduling.PodGroupUnschedulableType,
					Status: corev1.ConditionTrue,
					Reason: "NotEnoughResources",
				}},
			},
		},
	}

	updated, err := updater.UpdatePodGroup(desired, false)
	if err != nil {
		t.Fatalf("UpdatePodGroup() error = %v", err)
	}
	if statusUpdates != 2 {
		t.Fatalf("status updates = %d, want conflict retry", statusUpdates)
	}
	if updated.Status.Running != 2 || len(updated.Status.Conditions) != 1 || updated.Status.Conditions[0].Reason != schedulingapi.XPUTopologyPolicyConflictReason {
		t.Fatalf("updated status overwrote authoring blocker: %#v", updated.Status)
	}
}
