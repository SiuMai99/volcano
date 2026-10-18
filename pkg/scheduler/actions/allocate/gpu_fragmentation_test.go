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
	"reflect"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"

	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
	commonutil "volcano.sh/volcano/pkg/util"
)

type unsupportedFragmentationPredicate struct{}

func (*unsupportedFragmentationPredicate) Name() string { return "unsupported-fragmentation-predicate" }
func (p *unsupportedFragmentationPredicate) OnSessionOpen(ssn *framework.Session) {
	ssn.AddPredicateFn(p.Name(), func(*api.TaskInfo, *api.NodeInfo) error { return nil })
}
func (*unsupportedFragmentationPredicate) OnSessionClose(*framework.Session) {}

func fragmentationTask(gpu float64) *api.TaskInfo {
	return &api.TaskInfo{
		Pod: &v1.Pod{},
		InitResreq: &api.Resource{
			MilliCPU: 1000,
			Memory:   1024,
			ScalarResources: map[v1.ResourceName]float64{
				v1.ResourceName(api.GPUResourceName): gpu * gpuMilliPerCard,
				v1.ResourcePods:                      1,
			},
		},
	}
}

func TestDiagnoseGPUFragmentationWithPredicatesPlugin(t *testing.T) {
	pod := util.BuildPod("ns", "gpu-pod", "", v1.PodPending,
		api.BuildResourceList("1", "1Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "4"}),
		"pg", nil, map[string]string{"pool": "gpu"})
	test := uthelper.TestCommonStruct{
		Plugins: map[string]framework.PluginBuilder{predicates.PluginName: predicates.New},
		Pods:    []*v1.Pod{pod},
		PodGroups: []*schedulingv1.PodGroup{
			util.BuildPodGroup("pg", "ns", "queue", 1, nil, schedulingv1.PodGroupInqueue),
		},
		Queues: []*schedulingv1.Queue{util.BuildQueue("queue", 1, nil)},
		Nodes: []*v1.Node{
			util.BuildNode("n1", api.BuildResourceList("4", "8Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "8"}, api.ScalarResource{Name: "pods", Value: "10"}), map[string]string{"pool": "gpu"}),
			util.BuildNode("n2", api.BuildResourceList("4", "8Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "8"}, api.ScalarResource{Name: "pods", Value: "10"}), map[string]string{"pool": "gpu"}),
			util.BuildNode("n3", api.BuildResourceList("4", "8Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "8"}, api.ScalarResource{Name: "pods", Value: "10"}), map[string]string{"pool": "gpu"}),
		},
	}
	yes := true
	ssn := test.RegisterSession([]conf.Tier{{Plugins: []conf.PluginOption{{Name: predicates.PluginName, EnabledPredicate: &yes}}}}, nil)
	defer test.Close()
	var task *api.TaskInfo
	for _, job := range ssn.Jobs {
		for _, candidate := range job.Tasks {
			task = candidate
		}
	}
	if task == nil {
		t.Fatal("missing task")
	}
	if err := ssn.PrePredicateFn(task); err != nil {
		t.Fatalf("pre-predicate: %v", err)
	}
	for name, free := range map[string]float64{"n1": 1, "n2": 2, "n3": 1} {
		ssn.Nodes[name].Idle.SetScalar(v1.ResourceName(api.GPUResourceName), free*gpuMilliPerCard)
	}
	action := New()
	action.session = ssn
	if err := action.predicate(task, ssn.Nodes["n1"]); err == nil {
		t.Fatal("normal allocation predicate should stop at insufficient GPU")
	}
	if !ssn.SupportsGPUFragmentationDiagnosis() {
		t.Fatal("default predicates configuration should expose a diagnostic path")
	}
	got := diagnoseGPUFragmentation(task, ssn.NodeList, 10, ssn.GPUFragmentationPredicate)
	want := &gpuFragmentationDiagnosis{Requested: 4, CandidateNodes: 3, TotalFree: 4, MaxFree: 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diagnosis = %+v, want %+v", got, want)
	}
}

func TestAllocateGPUFragmentationMessage(t *testing.T) {
	previousShardingMode := options.ServerOpts.ShardingMode
	options.ServerOpts.ShardingMode = commonutil.NoneShardingMode
	defer func() { options.ServerOpts.ShardingMode = previousShardingMode }()
	for _, tc := range []struct {
		name         string
		enabled      bool
		want         bool
		free         map[string]float64
		n2LowCPU     bool
		n2WrongPool  bool
		n2Tainted    bool
		maxNodes     int
		cacheEnabled bool
		customPlugin bool
		expectPlaced bool
		releaseN1    float64
		pgPending    bool
		minAvailable int32
	}{
		{name: "enabled", enabled: true, want: true},
		{name: "disabled", enabled: false, want: false},
		{name: "insufficient total", enabled: true, free: map[string]float64{"n1": 1, "n2": 1, "n3": 1}},
		{name: "CPU blocks middle node", enabled: true, n2LowCPU: true},
		{name: "node selector blocks middle node", enabled: true, n2WrongPool: true},
		{name: "taint blocks middle node", enabled: true, n2Tainted: true},
		{name: "scan budget exceeded", enabled: true, maxNodes: 2},
		{name: "predicate cache unsupported", enabled: true, cacheEnabled: true},
		{name: "custom predicate unsupported", enabled: true, customPlugin: true},
		{name: "one node fits normally", enabled: true, free: map[string]float64{"n1": 4, "n2": 1, "n3": 1}, expectPlaced: true},
		{name: "future capacity is not current fragmentation", enabled: true, releaseN1: 3},
		{name: "queue admission pending", enabled: true, pgPending: true},
		{name: "Gang only pending", enabled: true, minAvailable: 2, free: map[string]float64{"n1": 4, "n2": 1, "n3": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := util.BuildPod("ns", "gpu-pod", "", v1.PodPending,
				api.BuildResourceList("1", "1Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "4"}),
				"pg", nil, map[string]string{"pool": "gpu"})
			test := uthelper.TestCommonStruct{
				Plugins: map[string]framework.PluginBuilder{predicates.PluginName: predicates.New},
				Pods:    []*v1.Pod{pod},
				Queues:  []*schedulingv1.Queue{util.BuildQueue("queue", 1, nil)},
			}
			minAvailable := tc.minAvailable
			if minAvailable == 0 {
				minAvailable = 1
			}
			phase := schedulingv1.PodGroupInqueue
			if tc.pgPending {
				phase = schedulingv1.PodGroupPending
			}
			test.PodGroups = []*schedulingv1.PodGroup{util.BuildPodGroup("pg", "ns", "queue", minAvailable, nil, phase)}
			for _, name := range []string{"n1", "n2", "n3"} {
				pool := "gpu"
				if name == "n2" && tc.n2WrongPool {
					pool = "cpu"
				}
				node := util.BuildNode(name,
					api.BuildResourceList("4", "8Gi", api.ScalarResource{Name: api.GPUResourceName, Value: "8"}, api.ScalarResource{Name: "pods", Value: "10"}),
					map[string]string{"pool": pool})
				if name == "n2" && tc.n2Tainted {
					node.Spec.Taints = []v1.Taint{{Key: "dedicated", Value: "other", Effect: v1.TaintEffectNoSchedule}}
				}
				test.Nodes = append(test.Nodes, node)
			}
			yes := true
			pluginArgs := map[string]interface{}{}
			if tc.cacheEnabled {
				pluginArgs[predicates.CachePredicate] = true
			}
			actionArgs := map[string]interface{}{conf.GPUFragmentationDiagnosisEnableKey: tc.enabled}
			if tc.maxNodes != 0 {
				actionArgs[conf.GPUFragmentationDiagnosisMaxNodesKey] = tc.maxNodes
			}
			pluginOptions := []conf.PluginOption{{Name: predicates.PluginName, EnabledPredicate: &yes, Arguments: pluginArgs}}
			if tc.minAvailable > 1 {
				test.Plugins[gang.PluginName] = gang.New
				pluginOptions = append([]conf.PluginOption{{
					Name: gang.PluginName, EnabledJobReady: &yes, EnabledSubJobReady: &yes,
					EnabledJobPipelined: &yes, EnabledSubJobPipelined: &yes,
				}}, pluginOptions...)
			}
			if tc.customPlugin {
				test.Plugins["unsupported-fragmentation-predicate"] = func(framework.Arguments) framework.Plugin {
					return &unsupportedFragmentationPredicate{}
				}
				pluginOptions = append(pluginOptions, conf.PluginOption{Name: "unsupported-fragmentation-predicate", EnabledPredicate: &yes})
			}
			ssn := test.RegisterSession([]conf.Tier{{Plugins: pluginOptions}},
				[]conf.Configuration{{Name: "allocate", Arguments: actionArgs}})
			defer test.Close()
			freeByNode := tc.free
			if freeByNode == nil {
				freeByNode = map[string]float64{"n1": 1, "n2": 2, "n3": 1}
			}
			for name, free := range freeByNode {
				ssn.Nodes[name].Idle.SetScalar(v1.ResourceName(api.GPUResourceName), free*gpuMilliPerCard)
			}
			if tc.n2LowCPU {
				ssn.Nodes["n2"].Idle.MilliCPU = 0
			}
			if tc.releaseN1 > 0 {
				ssn.Nodes["n1"].Releasing.SetScalar(v1.ResourceName(api.GPUResourceName), tc.releaseN1*gpuMilliPerCard)
			}
			if tc.pgPending {
				previousActions := conf.EnabledActionMap
				conf.EnabledActionMap = map[string]bool{"enqueue": true, "allocate": true}
				defer func() { conf.EnabledActionMap = previousActions }()
				New().Execute(ssn)
			} else {
				test.Run([]framework.Action{New()})
			}
			var task *api.TaskInfo
			var job *api.JobInfo
			for _, item := range ssn.Jobs {
				job = item
				for _, candidate := range item.Tasks {
					task = candidate
				}
			}
			if job == nil || task == nil {
				t.Fatal("missing scheduled job or task")
			}
			reason, msg, _ := job.TaskSchedulingReason(task.UID)
			if tc.expectPlaced {
				if task.Status == api.Pending || strings.Contains(msg, "GPU fragmentation across nodes") {
					t.Fatalf("expected normal placement, got status=%s message=%s", task.Status, msg)
				}
				return
			}
			if reason != api.PodReasonUnschedulable {
				t.Fatalf("reason = %q, want Unschedulable", reason)
			}
			hasDiagnosis := strings.Contains(msg, "GPU fragmentation across nodes: pod requests 4 nvidia.com/gpu; 3 otherwise-eligible nodes have 4 GPUs free in total, but at most 2 on any one node")
			if hasDiagnosis != tc.want {
				t.Fatalf("diagnosis present = %v, want %v; message: %s", hasDiagnosis, tc.want, msg)
			}
			if tc.releaseN1 > 0 {
				if task.Status != api.Pipelined {
					t.Fatalf("task status = %s, want Pipelined", task.Status)
				}
				return
			}
			if task.Status != api.Pending {
				t.Fatalf("task status = %s, want Pending", task.Status)
			}
		})
	}
}

func TestCompleteGPUFragmentationNodeScope(t *testing.T) {
	all := []*api.NodeInfo{{Name: "n1"}, {Name: "n2"}}
	for _, tc := range []struct {
		name  string
		nodes []*api.NodeInfo
		want  bool
	}{
		{name: "complete", nodes: []*api.NodeInfo{{Name: "n2"}, {Name: "n1"}}, want: true},
		{name: "missing", nodes: []*api.NodeInfo{{Name: "n1"}}},
		{name: "duplicate", nodes: []*api.NodeInfo{{Name: "n1"}, {Name: "n1"}}},
		{name: "unknown", nodes: []*api.NodeInfo{{Name: "n1"}, {Name: "n3"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := completeGPUFragmentationNodeScope(tc.nodes, all); got != tc.want {
				t.Fatalf("complete scope = %v, want %v", got, tc.want)
			}
		})
	}
}

func fragmentationNode(name string, free float64) *api.NodeInfo {
	return &api.NodeInfo{
		Name: name,
		Allocatable: &api.Resource{ScalarResources: map[v1.ResourceName]float64{
			v1.ResourceName(api.GPUResourceName): 8 * gpuMilliPerCard,
		}},
		Idle: &api.Resource{MilliCPU: 4000, Memory: 8192, ScalarResources: map[v1.ResourceName]float64{
			v1.ResourceName(api.GPUResourceName): free * gpuMilliPerCard,
			v1.ResourcePods:                      10,
		}},
		Releasing: api.EmptyResource(),
		Pipelined: api.EmptyResource(),
	}
}

func TestDiagnoseGPUFragmentation(t *testing.T) {
	tests := []struct {
		name      string
		free      []float64
		modify    func(*api.TaskInfo, []*api.NodeInfo)
		predicate gpuFragmentationPredicate
		maxNodes  int
		want      *gpuFragmentationDiagnosis
	}{
		{name: "fragmented", free: []float64{1, 2, 1}, want: &gpuFragmentationDiagnosis{Requested: 4, CandidateNodes: 3, TotalFree: 4, MaxFree: 2}},
		{name: "insufficient total", free: []float64{1, 1, 1}},
		{name: "one node fits", free: []float64{1, 4, 1}},
		{name: "non GPU resource fails", free: []float64{1, 2, 1}, modify: func(_ *api.TaskInfo, nodes []*api.NodeInfo) {
			nodes[1].Idle.MilliCPU = 0
		}},
		{name: "node affinity fails", free: []float64{1, 2, 1}, predicate: func(_ *api.TaskInfo, node *api.NodeInfo) (bool, bool) {
			return node.Name != "n2", true
		}},
		{name: "predicate unknown", free: []float64{1, 2, 1}, predicate: func(_ *api.TaskInfo, node *api.NodeInfo) (bool, bool) {
			return node.Name != "n2", node.Name != "n2"
		}},
		{name: "future capacity on one node", free: []float64{1, 2, 1}, modify: func(_ *api.TaskInfo, nodes []*api.NodeInfo) {
			nodes[0].Releasing.SetScalar(v1.ResourceName(api.GPUResourceName), 3*gpuMilliPerCard)
		}},
		{name: "invalid idle", free: []float64{1, 2, 1.5}},
		{name: "budget exhausted", free: []float64{1, 2, 1}, maxNodes: 2},
		{name: "mixed scalar request", free: []float64{1, 2, 1}, modify: func(task *api.TaskInfo, _ []*api.NodeInfo) {
			task.InitResreq.SetScalar("example.com/fpga", 1)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := fragmentationTask(4)
			nodes := make([]*api.NodeInfo, len(tc.free))
			for i, free := range tc.free {
				nodes[i] = fragmentationNode("n"+string(rune('1'+i)), free)
			}
			if tc.modify != nil {
				tc.modify(task, nodes)
			}
			requestBefore := task.InitResreq.Clone()
			idleBefore := make([]*api.Resource, len(nodes))
			for i := range nodes {
				idleBefore[i] = nodes[i].Idle.Clone()
			}
			predicate := tc.predicate
			if predicate == nil {
				predicate = func(*api.TaskInfo, *api.NodeInfo) (bool, bool) { return true, true }
			}
			limit := tc.maxNodes
			if limit == 0 {
				limit = 10
			}
			got := diagnoseGPUFragmentation(task, nodes, limit, predicate)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("diagnosis = %+v, want %+v", got, tc.want)
			}
			if !reflect.DeepEqual(task.InitResreq, requestBefore) {
				t.Fatal("diagnosis mutated the task request")
			}
			for i := range nodes {
				if !reflect.DeepEqual(nodes[i].Idle, idleBefore[i]) {
					t.Fatalf("diagnosis mutated node %s idle resources", nodes[i].Name)
				}
			}
		})
	}
}

func BenchmarkDiagnoseGPUFragmentation512Nodes(b *testing.B) {
	task := fragmentationTask(4)
	nodes := make([]*api.NodeInfo, 512)
	for i := range nodes {
		nodes[i] = fragmentationNode(fmt.Sprintf("gpu-node-%d", i), 0)
	}
	predicate := func(*api.TaskInfo, *api.NodeInfo) (bool, bool) { return true, true }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := diagnoseGPUFragmentation(task, nodes, 512, predicate); got != nil {
			b.Fatalf("unexpected diagnosis: %+v", got)
		}
	}
}
