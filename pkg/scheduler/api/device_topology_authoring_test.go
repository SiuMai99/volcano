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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestJobInfoRebuildsCanonicalDeviceTopologyView(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "worker-0",
			UID:       types.UID("worker-0"),
			Labels:    map[string]string{"role": "worker"},
			Annotations: map[string]string{
				schedulingv1beta1.KubeGroupNameAnnotationKey: "train",
				"volcano.sh/xpu-topology":                    `{"must-not-be-an-authoring-source":true}`,
			},
		},
	}
	job := NewJobInfo("default/train", NewTaskInfo(pod))
	if job.DeviceTopologyValid || !job.DeviceTopology.Empty() || job.DeviceTopologyFingerprint != "" {
		t.Fatalf("Pod annotation unexpectedly created device topology policy: %#v", job)
	}

	initial := &PodGroup{PodGroup: scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "train"},
		Spec: scheduling.PodGroupSpec{
			DeviceTopology: deviceTopologyForAuthoringTest("node-a"),
			SubGroupPolicy: []scheduling.SubGroupPolicySpec{{
				Name:           "workers",
				LabelSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"role": "worker"}},
				MatchLabelKeys: []string{"role"},
				DeviceTopology: deviceTopologyForAuthoringTest("node-a"),
			}},
		},
	}}
	job.SetPodGroup(initial)

	if !job.DeviceTopologyValid || job.DeviceTopologyFingerprint == "" || job.DeviceTopology.Empty() {
		t.Fatalf("top-level policy was not canonicalized: %#v, %q, valid=%t", job.DeviceTopology, job.DeviceTopologyFingerprint, job.DeviceTopologyValid)
	}
	firstSubJob := onlySubJob(t, job)
	if !firstSubJob.DeviceTopologyValid || firstSubJob.DeviceTopologyFingerprint == "" {
		t.Fatalf("subgroup policy was not canonicalized: %#v", firstSubJob)
	}

	clone := job.Clone()
	if !clone.DeviceTopology.Equal(job.DeviceTopology) || clone.DeviceTopologyFingerprint != job.DeviceTopologyFingerprint || !clone.DeviceTopologyValid {
		t.Fatalf("clone lost canonical top-level policy: %#v", clone)
	}

	changed := initial.Clone()
	changed.Spec.DeviceTopology = deviceTopologyForAuthoringTest("node-b")
	changed.Spec.SubGroupPolicy[0].DeviceTopology = deviceTopologyForAuthoringTest("node-b")
	job.SetPodGroup(changed)

	secondSubJob := onlySubJob(t, job)
	if firstSubJob == secondSubJob {
		t.Fatal("semantic policy update did not rebuild the subgroup view")
	}
	if job.DeviceTopologyFingerprint == clone.DeviceTopologyFingerprint || secondSubJob.DeviceTopologyFingerprint == firstSubJob.DeviceTopologyFingerprint {
		t.Fatal("semantic policy update did not invalidate canonical fingerprints")
	}

	job.UnsetPodGroup()
	if job.DeviceTopologyValid || !job.DeviceTopology.Empty() || job.DeviceTopologyFingerprint != "" {
		t.Fatalf("UnsetPodGroup retained policy state: %#v, %q, valid=%t", job.DeviceTopology, job.DeviceTopologyFingerprint, job.DeviceTopologyValid)
	}
}

func deviceTopologyForAuthoringTest(class string) *scheduling.DeviceTopologySpec {
	return &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{{
		ResourceName: "example.com/gpu",
		Domain: scheduling.DeviceTopologyDomainSelector{
			Scope:       scheduling.DeviceTopologyDomainScopeNode,
			DomainClass: scheduling.DeviceTopologyDomainClass(class),
		},
	}}}
}

func onlySubJob(t *testing.T, job *JobInfo) *SubJobInfo {
	t.Helper()
	if len(job.SubJobs) != 1 {
		t.Fatalf("subjob count = %d, want 1: %#v", len(job.SubJobs), job.SubJobs)
	}
	for _, subJob := range job.SubJobs {
		return subJob
	}
	t.Fatal("unreachable")
	return nil
}
