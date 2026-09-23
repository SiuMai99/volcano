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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	testGPUResource corev1.ResourceName = "nvidia.com/gpu"
	testNICResource corev1.ResourceName = "example.com/rdma"
)

func TestXPUAssignmentCanonicalRoundTrip(t *testing.T) {
	assignment := XPUAssignment{
		Version: XPUAssignmentVersion,
		Assignments: []XPUAssignmentEntry{
			{
				Container:    XPUContainerRef{Kind: XPUContainerRegular, Name: "worker"},
				ResourceName: testGPUResource,
				Provider:     "nvidia-nvml-v1",
				DeviceKeys:   []string{"node-b/GPU-B", "node-a/GPU-A"},
			},
			{
				Container:    XPUContainerRef{Kind: XPUContainerInit, Name: "prepare"},
				ResourceName: testNICResource,
				Provider:     "rdma-v1",
				DeviceKeys:   []string{"node-a/NIC-A"},
			},
		},
	}

	data, err := MarshalXPUAssignment(assignment)
	if err != nil {
		t.Fatalf("MarshalXPUAssignment() error = %v", err)
	}
	parsed, err := ParseXPUAssignment(data)
	if err != nil {
		t.Fatalf("ParseXPUAssignment() error = %v", err)
	}
	if got, want := parsed.Assignments[0].ResourceName, testNICResource; got != want {
		t.Fatalf("first resource = %q, want %q", got, want)
	}
	if got, want := parsed.Assignments[1].DeviceKeys, []string{"node-a/GPU-A", "node-b/GPU-B"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("GPU keys = %#v, want %#v", got, want)
	}
	second, err := MarshalXPUAssignment(parsed)
	if err != nil {
		t.Fatalf("second MarshalXPUAssignment() error = %v", err)
	}
	if string(second) != string(data) {
		t.Fatalf("canonical round trip changed JSON:\nfirst:  %s\nsecond: %s", data, second)
	}
}

func TestParseXPUDeviceKeyUsesTrustedProviderNamespace(t *testing.T) {
	key, err := ParseXPUDeviceKey(
		testGPUResource,
		"nvidia-nvml-v1",
		"inventory.example.io",
		"node-uid/GPU-AAAAAAAA",
	)
	if err != nil {
		t.Fatalf("ParseXPUDeviceKey() error = %v", err)
	}
	if got, want := key.ID.Namespace, "inventory.example.io"; got != want {
		t.Fatalf("Provider namespace = %q, want trusted namespace %q", got, want)
	}
	if got, resourcePrefix := key.ID.Namespace, "nvidia.com"; got == resourcePrefix {
		t.Fatalf("Provider namespace must not be inferred from resource prefix %q", resourcePrefix)
	}
}

func TestParseXPUAssignmentRejectsInvalidPayloads(t *testing.T) {
	base := `{"version":1,"assignments":[{"container":{"kind":"regular","name":"worker"},"resourceName":"nvidia.com/gpu","provider":"nvidia-nvml-v1","deviceKeys":["node-a/GPU-A"]}]}`
	tests := []struct {
		name      string
		payload   string
		wantError string
	}{
		{name: "empty", payload: ``, wantError: "empty input"},
		{name: "unknown version", payload: strings.Replace(base, `"version":1`, `"version":2`, 1), wantError: "unsupported assignment version"},
		{name: "unknown field", payload: strings.Replace(base, `"version":1`, `"version":1,"planDigest":"x"`, 1), wantError: "unknown field"},
		{name: "duplicate JSON field", payload: strings.Replace(base, `"provider":"nvidia-nvml-v1"`, `"provider":"nvidia-nvml-v1","provider":"other"`, 1), wantError: "duplicate JSON object key"},
		{name: "trailing JSON", payload: base + `{}`, wantError: "trailing JSON value"},
		{name: "empty assignments", payload: `{"version":1,"assignments":[]}`, wantError: "assignments is empty"},
		{name: "unknown kind", payload: strings.Replace(base, `"kind":"regular"`, `"kind":"sidecar"`, 1), wantError: "container kind"},
		{name: "unqualified resource", payload: strings.Replace(base, `nvidia.com/gpu`, `gpu`, 1), wantError: "qualified extended resource"},
		{name: "empty provider", payload: strings.Replace(base, `nvidia-nvml-v1`, ``, 1), wantError: "provider is empty"},
		{name: "duplicate key", payload: strings.Replace(base, `"node-a/GPU-A"`, `"node-a/GPU-A","node-a/GPU-A"`, 1), wantError: "is duplicated"},
		{name: "noncanonical key", payload: strings.Replace(base, `node-a/GPU-A`, ` node-a/GPU-A `, 1), wantError: "is not canonical"},
		{name: "duplicate resource", payload: strings.Replace(base, `}]}`, `},{"container":{"kind":"init","name":"prepare"},"resourceName":"nvidia.com/gpu","provider":"nvidia-nvml-v1","deviceKeys":["node-a/GPU-B"]}]}`, 1), wantError: "resource name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseXPUAssignment([]byte(tt.payload))
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("ParseXPUAssignment() error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func TestXPUResourceRequestForPodPreservesContainerRef(t *testing.T) {
	restartAlways := corev1.ContainerRestartPolicyAlways
	quantity := *resource.NewQuantity(2, resource.DecimalSI)
	tests := []struct {
		name           string
		containers     []corev1.Container
		initContainers []corev1.Container
		want           XPUResourceRequest
	}{
		{
			name: "regular",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{testGPUResource: quantity},
			}}},
			want: XPUResourceRequest{Container: XPUContainerRef{Kind: XPUContainerRegular, Name: "worker"}, ResourceName: testGPUResource, Count: 2},
		},
		{
			name:       "init",
			containers: []corev1.Container{{Name: "worker"}},
			initContainers: []corev1.Container{{Name: "prepare", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{testGPUResource: quantity},
			}}},
			want: XPUResourceRequest{Container: XPUContainerRef{Kind: XPUContainerInit, Name: "prepare"}, ResourceName: testGPUResource, Count: 2},
		},
		{
			name:       "restartable init",
			containers: []corev1.Container{{Name: "worker"}},
			initContainers: []corev1.Container{{Name: "device-sidecar", RestartPolicy: &restartAlways, Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{testGPUResource: quantity},
				Limits:   corev1.ResourceList{testGPUResource: quantity},
			}}},
			want: XPUResourceRequest{Container: XPUContainerRef{Kind: XPUContainerRestartableInit, Name: "device-sidecar"}, ResourceName: testGPUResource, Count: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: tt.containers, InitContainers: tt.initContainers}}
			got, err := XPUResourceRequestForPod(pod, testGPUResource)
			if err != nil {
				t.Fatalf("XPUResourceRequestForPod() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("XPUResourceRequestForPod() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestXPUResourceRequestForPodRejectsUnsupportedShapes(t *testing.T) {
	integerTwo := *resource.NewQuantity(2, resource.DecimalSI)
	fractional := *resource.NewMilliQuantity(1500, resource.DecimalSI)
	tests := []struct {
		name       string
		containers []corev1.Container
		wantError  string
	}{
		{
			name: "multiple consumers",
			containers: []corev1.Container{
				{Name: "worker", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{testGPUResource: integerTwo}}},
				{Name: "other", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{testGPUResource: integerTwo}}},
			},
			wantError: "only one container",
		},
		{
			name: "request without limit",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{testGPUResource: integerTwo},
			}}},
			wantError: "limit must be set",
		},
		{
			name: "fractional limit",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{testGPUResource: fractional},
			}}},
			wantError: "positive integral",
		},
		{
			name: "request differs from limit",
			containers: []corev1.Container{{Name: "worker", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{testGPUResource: *resource.NewQuantity(1, resource.DecimalSI)},
				Limits:   corev1.ResourceList{testGPUResource: integerTwo},
			}}},
			wantError: "request must equal limit",
		},
		{name: "missing consumer", containers: []corev1.Container{{Name: "worker"}}, wantError: "exactly one container"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: tt.containers}}
			_, err := XPUResourceRequestForPod(pod, testGPUResource)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("XPUResourceRequestForPod() error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateXPUAssignmentForPodAndNodeUID(t *testing.T) {
	quantity := *resource.NewQuantity(2, resource.DecimalSI)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "default", UID: types.UID("pod-uid")},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "worker",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{testGPUResource: quantity},
			},
		}}},
	}
	assignment := XPUAssignment{Version: XPUAssignmentVersion, Assignments: []XPUAssignmentEntry{{
		Container:    XPUContainerRef{Kind: XPUContainerRegular, Name: "worker"},
		ResourceName: testGPUResource,
		Provider:     "nvidia-nvml-v1",
		DeviceKeys:   []string{"node-uid/GPU-A", "node-uid/GPU-B"},
	}}}
	if err := ValidateXPUAssignmentForPod(pod, assignment); err != nil {
		t.Fatalf("ValidateXPUAssignmentForPod() error = %v", err)
	}
	if err := ValidateXPUAssignmentNodeUID(assignment, types.UID("node-uid")); err != nil {
		t.Fatalf("ValidateXPUAssignmentNodeUID() error = %v", err)
	}

	wrongContainer := assignment
	wrongContainer.Assignments = append([]XPUAssignmentEntry(nil), assignment.Assignments...)
	wrongContainer.Assignments[0].Container.Name = "other"
	if err := ValidateXPUAssignmentForPod(pod, wrongContainer); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong container error = %v", err)
	}

	wrongCount := assignment
	wrongCount.Assignments = append([]XPUAssignmentEntry(nil), assignment.Assignments...)
	wrongCount.Assignments[0].DeviceKeys = []string{"node-uid/GPU-A"}
	if err := ValidateXPUAssignmentForPod(pod, wrongCount); err == nil || !strings.Contains(err.Error(), "does not match limit") {
		t.Fatalf("wrong count error = %v", err)
	}

	if err := ValidateXPUAssignmentNodeUID(assignment, types.UID("replacement-uid")); err == nil || !strings.Contains(err.Error(), "replacement-uid") {
		t.Fatalf("NodeUID replacement error = %v", err)
	}
}
