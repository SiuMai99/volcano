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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestGPUFragmentationUsesExistingPodFailureChannel(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	pod := buildPod("ns", "gpu-pod", "", v1.PodPending,
		api.BuildResourceList("1", "1Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "4"}), nil, nil)
	if _, err := sc.kubeClient.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Pod: %v", err)
	}
	task := api.NewTaskInfo(pod)
	job := api.NewJobInfo("ns/gpu-pod", task)
	job.MinAvailable = 1
	fit := api.NewFitErrors()
	fit.SetError("GPU fragmentation across nodes: pod requests 4 nvidia.com/gpu; 3 otherwise-eligible nodes have 4 GPUs free in total, but at most 2 on any one node")
	for _, name := range []string{"n1", "n2", "n3"} {
		fit.SetNodeError(name, api.NewFitErrWithStatus(task, &api.NodeInfo{Name: name}, &api.Status{Code: api.Unschedulable, Reason: "Insufficient nvidia.com/gpu"}))
	}
	job.NodesFitErrors[task.UID] = fit
	sc.RecordJobStatusEvent(job, false)
	updated, err := sc.kubeClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated Pod: %v", err)
	}
	if len(updated.Status.Conditions) != 1 {
		t.Fatalf("Pod conditions = %+v, want one PodScheduled condition", updated.Status.Conditions)
	}
	condition := updated.Status.Conditions[0]
	if condition.Type != v1.PodScheduled || condition.Status != v1.ConditionFalse || condition.Reason != api.PodReasonUnschedulable ||
		!strings.Contains(condition.Message, "GPU fragmentation across nodes: pod requests 4 nvidia.com/gpu") {
		t.Fatalf("unexpected PodScheduled condition: %+v", condition)
	}
	recorder := sc.Recorder.(*record.FakeRecorder)
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "FailedScheduling") || !strings.Contains(event, "GPU fragmentation across nodes") {
			t.Fatalf("unexpected event: %s", event)
		}
	default:
		t.Fatal("missing FailedScheduling event")
	}

	// A later cycle with the same message must use the existing condition
	// equality check instead of emitting a duplicate event.
	task.Pod = updated
	sc.RecordJobStatusEvent(job, false)
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected duplicate event: %s", event)
	default:
	}
}
