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
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// XPUAssignmentWriter is the narrow API-Pod persistence boundary needed by a
// later final assignment path. PR9 provides the contract and PoC only; callers
// must not invoke it from trial planning or scoring.
type XPUAssignmentWriter interface {
	Write(context.Context, *corev1.Pod, api.XPUAssignment) (*corev1.Pod, error)
}

// APIPodXPUAssignmentWriter persists one canonical assignment with UID and
// resourceVersion preconditions. It performs no conflict retry: a caller must
// discard the stale plan and re-enter scheduling with a fresh snapshot.
type APIPodXPUAssignmentWriter struct {
	kubeClient kubernetes.Interface
}

func NewAPIPodXPUAssignmentWriter(kubeClient kubernetes.Interface) *APIPodXPUAssignmentWriter {
	return &APIPodXPUAssignmentWriter{kubeClient: kubeClient}
}

type xpuAssignmentJSONPatchOperation struct {
	Operation string      `json:"op"`
	Path      string      `json:"path"`
	Value     interface{} `json:"value,omitempty"`
}

func (w *APIPodXPUAssignmentWriter) Write(ctx context.Context, pod *corev1.Pod, assignment api.XPUAssignment) (*corev1.Pod, error) {
	if w == nil || w.kubeClient == nil {
		return nil, fmt.Errorf("write xPU assignment: Kubernetes client is unavailable")
	}
	if pod == nil {
		return nil, fmt.Errorf("write xPU assignment: Pod is required")
	}
	if pod.Namespace == "" || pod.Name == "" || pod.UID == "" || pod.ResourceVersion == "" {
		return nil, fmt.Errorf("write xPU assignment for %s/%s: Pod namespace, name, UID and resourceVersion are required", pod.Namespace, pod.Name)
	}
	if err := api.ValidateXPUAssignmentForPod(pod, assignment); err != nil {
		return nil, fmt.Errorf("write xPU assignment for %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	value, err := api.MarshalXPUAssignment(assignment)
	if err != nil {
		return nil, fmt.Errorf("write xPU assignment for %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	operations := []xpuAssignmentJSONPatchOperation{
		{Operation: "test", Path: "/metadata/uid", Value: string(pod.UID)},
		{Operation: "test", Path: "/metadata/resourceVersion", Value: pod.ResourceVersion},
	}
	if pod.Annotations == nil {
		operations = append(operations, xpuAssignmentJSONPatchOperation{
			Operation: "add",
			Path:      "/metadata/annotations",
			Value:     map[string]string{api.XPUAssignmentAnnotationKey: string(value)},
		})
	} else {
		operations = append(operations, xpuAssignmentJSONPatchOperation{
			Operation: "add",
			Path:      "/metadata/annotations/" + escapeJSONPointerToken(api.XPUAssignmentAnnotationKey),
			Value:     string(value),
		})
	}
	patch, err := json.Marshal(operations)
	if err != nil {
		return nil, fmt.Errorf("write xPU assignment for %s/%s: encode JSON patch: %w", pod.Namespace, pod.Name, err)
	}

	updated, err := w.kubeClient.CoreV1().Pods(pod.Namespace).Patch(
		ctx,
		pod.Name,
		types.JSONPatchType,
		patch,
		metav1.PatchOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("write xPU assignment for %s/%s at resourceVersion %s: %w", pod.Namespace, pod.Name, pod.ResourceVersion, err)
	}
	if updated == nil || updated.UID != pod.UID {
		return nil, fmt.Errorf("write xPU assignment for %s/%s: API server returned a different Pod identity", pod.Namespace, pod.Name)
	}
	if updated.Annotations[api.XPUAssignmentAnnotationKey] != string(value) {
		return nil, fmt.Errorf("write xPU assignment for %s/%s: API server response does not contain the canonical assignment", pod.Namespace, pod.Name)
	}
	return updated, nil
}

func escapeJSONPointerToken(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}
