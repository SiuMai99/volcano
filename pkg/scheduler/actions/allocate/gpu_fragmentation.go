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

package allocate

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	gpuMilliPerCard  = 1000
	maxExactGPUMilli = 1<<53 - 1
	maxExactGPUCount = maxExactGPUMilli / gpuMilliPerCard
)

// gpuFragmentationDiagnosis is evidence about one Pod at one point in a
// scheduling session. It has no effect on node selection or allocation.
type gpuFragmentationDiagnosis struct {
	Requested      int64
	CandidateNodes int
	TotalFree      int64
	MaxFree        int64
}

func (d *gpuFragmentationDiagnosis) Message() string {
	return fmt.Sprintf("GPU fragmentation across nodes: pod requests %d nvidia.com/gpu; %d otherwise-eligible nodes have %d GPUs free in total, but at most %d on any one node",
		d.Requested, d.CandidateNodes, d.TotalFree, d.MaxFree)
}

// completeGPUFragmentationNodeScope prevents a partial topology or shard view
// from being reported as a cluster-wide diagnosis.
func completeGPUFragmentationNodeScope(nodes, all []*api.NodeInfo) bool {
	if len(nodes) == 0 || len(nodes) != len(all) {
		return false
	}
	names := make(map[string]struct{}, len(all))
	for _, node := range all {
		if node == nil {
			return false
		}
		names[node.Name] = struct{}{}
	}
	if len(names) != len(all) {
		return false
	}
	for _, node := range nodes {
		if node == nil {
			return false
		}
		if _, found := names[node.Name]; !found {
			return false
		}
		delete(names, node.Name)
	}
	return len(names) == 0
}

type gpuFragmentationPredicate func(*api.TaskInfo, *api.NodeInfo) (fits, known bool)

func wholeGPUCount(value float64) (int64, bool) {
	// Resource.NewResource records extended scalar resources in milli-units.
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > maxExactGPUMilli || math.Mod(value, gpuMilliPerCard) != 0 {
		return 0, false
	}
	return int64(value / gpuMilliPerCard), true
}

// diagnoseGPUFragmentation inspects every GPU-capable node in the supplied
// complete scope. Unknown evidence aborts the entire diagnosis. The caller
// must have already proved that every enabled predicate has a diagnostic path.
func diagnoseGPUFragmentation(task *api.TaskInfo, nodes []*api.NodeInfo, maxNodes int, predicate gpuFragmentationPredicate) *gpuFragmentationDiagnosis {
	if task == nil || task.Pod == nil || task.InitResreq == nil || predicate == nil || maxNodes <= 0 || len(nodes) == 0 || len(nodes) > maxNodes {
		return nil
	}
	if len(task.Pod.Spec.ResourceClaims) != 0 {
		return nil
	}

	gpuName := v1.ResourceName(api.GPUResourceName)
	requested, valid := wholeGPUCount(task.InitResreq.ScalarResources[gpuName])
	if !valid || requested == 0 {
		return nil
	}
	// The first release only covers an ordinary whole-GPU request. Other
	// scalar resources can carry device-specific placement semantics.
	for name, value := range task.InitResreq.ScalarResources {
		if name != gpuName && name != v1.ResourcePods && value != 0 {
			return nil
		}
	}

	nonGPURequest := task.InitResreq.Clone()
	delete(nonGPURequest.ScalarResources, gpuName)
	result := &gpuFragmentationDiagnosis{Requested: requested}
	for _, node := range nodes {
		if node == nil || node.Allocatable == nil || node.Idle == nil || node.Releasing == nil || node.Pipelined == nil {
			return nil
		}
		capacity, ok := wholeGPUCount(node.Allocatable.ScalarResources[gpuName])
		if !ok {
			return nil
		}
		if capacity == 0 {
			continue
		}
		free, ok := wholeGPUCount(node.Idle.ScalarResources[gpuName])
		if !ok || free > capacity {
			return nil
		}
		if !nonGPURequest.LessEqual(node.Idle, api.Zero) {
			continue
		}
		fits, known := predicate(task, node)
		if !known {
			return nil
		}
		if !fits {
			continue
		}
		// A future fit can explain Pending through pipelining or another
		// failure path; current-idle fragmentation is not the sole cause.
		futureFree, ok := wholeGPUCount(node.FutureIdle().ScalarResources[gpuName])
		if !ok || futureFree >= requested {
			return nil
		}
		result.CandidateNodes++
		if result.TotalFree > maxExactGPUCount-free {
			return nil
		}
		result.TotalFree += free
		if free > result.MaxFree {
			result.MaxFree = free
		}
	}
	if result.CandidateNodes == 0 || result.TotalFree < requested || result.MaxFree >= requested {
		return nil
	}
	return result
}
