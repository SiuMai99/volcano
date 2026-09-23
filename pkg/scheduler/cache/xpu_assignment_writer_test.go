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
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestAPIPodXPUAssignmentWriterPersistsCanonicalAnnotation(t *testing.T) {
	pod := assignmentWriterTestPod()
	pod.Annotations = map[string]string{"keep": "value", api.XPUAssignmentAnnotationKey: "user-value"}
	kubeClient := kubefake.NewSimpleClientset(pod.DeepCopy())
	writer := NewAPIPodXPUAssignmentWriter(kubeClient)
	assignment := assignmentWriterTestAssignment()

	updated, err := writer.Write(context.Background(), pod.DeepCopy(), assignment)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	canonical, err := api.MarshalXPUAssignment(assignment)
	if err != nil {
		t.Fatalf("MarshalXPUAssignment() error = %v", err)
	}
	if got := updated.Annotations[api.XPUAssignmentAnnotationKey]; got != string(canonical) {
		t.Fatalf("updated assignment = %q, want %q", got, canonical)
	}
	if got := updated.Annotations["keep"]; got != "value" {
		t.Fatalf("unrelated annotation = %q, want value", got)
	}
	stored, err := kubeClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("GET persisted Pod error = %v", err)
	}
	if got := stored.Annotations[api.XPUAssignmentAnnotationKey]; got != string(canonical) {
		t.Fatalf("persisted assignment = %q, want %q", got, canonical)
	}

	// Repeating the exact canonical write is safe and preserves the same value.
	if _, err := writer.Write(context.Background(), stored, assignment); err != nil {
		t.Fatalf("idempotent Write() error = %v", err)
	}
}

func TestAPIPodXPUAssignmentWriterInitializesAnnotations(t *testing.T) {
	pod := assignmentWriterTestPod()
	pod.Annotations = nil
	kubeClient := kubefake.NewSimpleClientset(pod.DeepCopy())
	writer := NewAPIPodXPUAssignmentWriter(kubeClient)
	if _, err := writer.Write(context.Background(), pod, assignmentWriterTestAssignment()); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	stored, err := kubeClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("GET persisted Pod error = %v", err)
	}
	if stored.Annotations[api.XPUAssignmentAnnotationKey] == "" {
		t.Fatal("assignment annotation was not initialized")
	}
}

func TestAPIPodXPUAssignmentWriterRejectsStaleIdentity(t *testing.T) {
	pod := assignmentWriterTestPod()
	kubeClient := kubefake.NewSimpleClientset(pod.DeepCopy())
	writer := NewAPIPodXPUAssignmentWriter(kubeClient)
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "resourceVersion conflict", mutate: func(candidate *corev1.Pod) { candidate.ResourceVersion = "stale" }},
		{name: "UID replacement", mutate: func(candidate *corev1.Pod) { candidate.UID = "replacement-uid" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := pod.DeepCopy()
			tt.mutate(candidate)
			if _, err := writer.Write(context.Background(), candidate, assignmentWriterTestAssignment()); err == nil {
				t.Fatal("Write() should reject a stale Pod identity")
			}
		})
	}
}

func TestAPIPodXPUAssignmentWriterValidatesBeforePatch(t *testing.T) {
	pod := assignmentWriterTestPod()
	kubeClient := kubefake.NewSimpleClientset(pod.DeepCopy())
	writer := NewAPIPodXPUAssignmentWriter(kubeClient)
	assignment := assignmentWriterTestAssignment()
	assignment.Assignments[0].Container.Name = "other"
	if _, err := writer.Write(context.Background(), pod, assignment); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("Write() error = %v, want container mismatch", err)
	}
	if got := len(kubeClient.Actions()); got != 0 {
		t.Fatalf("invalid assignment reached Kubernetes client with %d actions", got)
	}
}

func assignmentWriterTestPod() *corev1.Pod {
	quantity := *resource.NewQuantity(1, resource.DecimalSI)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "worker-0",
			UID:             types.UID("pod-uid-a"),
			ResourceVersion: "7",
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "worker",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{"nvidia.com/gpu": quantity},
			},
		}}},
	}
}

func assignmentWriterTestAssignment() api.XPUAssignment {
	return api.XPUAssignment{
		Version: api.XPUAssignmentVersion,
		Assignments: []api.XPUAssignmentEntry{{
			Container:    api.XPUContainerRef{Kind: api.XPUContainerRegular, Name: "worker"},
			ResourceName: "nvidia.com/gpu",
			Provider:     "nvidia-nvml-v1",
			DeviceKeys:   []string{"node-uid-a/GPU-AAAAAAAA"},
		}},
	}
}
