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

package xputopologyaware

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/topology"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

func TestOnSessionOpenWiresCompilerAndAdvisoryScorer(t *testing.T) {
	schedulerCache := cache.NewDefaultMockSchedulerCache("volcano")
	observedAt := time.Now()
	catalog := integrationCatalog(t)
	manager := schedulerCache.XPUTopologyManager()
	if err := manager.Configure(topology.ActivationConfig{
		GateEnabled: true, PluginConfigured: true, PluginConfigReady: true, ConfigurationIdentity: "integration-test",
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if err := manager.ConfigureCatalog(catalog); err != nil {
		t.Fatalf("ConfigureCatalog() error = %v", err)
	}

	identity := provider.ProviderIdentityRef{ProviderID: "mock", Namespace: "example.com"}
	ingestor, err := topology.NewFactsIngestor(catalog, provider.TopologyProviderCapabilities{
		Identity: identity, ResourceName: compilerTestResource,
		DomainClasses: []api.DomainClassKey{{
			ResourceName: compilerTestResource, Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up",
		}},
	})
	if err != nil {
		t.Fatalf("NewFactsIngestor() error = %v", err)
	}
	if err := schedulerCache.ConfigureXPUTopologyFactsIngestor(ingestor); err != nil {
		t.Fatalf("ConfigureXPUTopologyFactsIngestor() error = %v", err)
	}

	for nodeName, count := range map[string]int{"node-a": 1, "node-b": 2} {
		node := integrationNode(nodeName)
		if err := schedulerCache.AddOrUpdateNode(node); err != nil {
			t.Fatalf("AddOrUpdateNode(%s) error = %v", nodeName, err)
		}
		facts := provider.NodeTopologyFacts{ResourceName: compilerTestResource}
		deviceIDs := make([]api.SourceDeviceID, 0, count)
		for index := 0; index < count; index++ {
			deviceID := api.SourceDeviceID(fmt.Sprintf("%s-gpu-%d", nodeName, index))
			facts.Devices = append(facts.Devices, provider.DeviceFact{ID: deviceID, Health: api.DeviceHealthy})
			deviceIDs = append(deviceIDs, deviceID)
		}
		facts.LocalDomains = append(facts.LocalDomains, provider.LocalDomainFact{
			ID: "domain-" + api.SourceDomainID(nodeName), DomainClass: "local-scale-up", DeviceIDs: deviceIDs,
		})
		update := provider.MockProvider{Identity: identity}.Replace(
			api.NodeObservation{Identity: api.NodeIdentity{Name: node.Name, UID: node.UID}, ResourceVersion: node.ResourceVersion},
			1, facts, observedAt, observedAt.Add(time.Hour),
		)
		if err := schedulerCache.ApplyXPUTopologyUpdate(update); err != nil {
			t.Fatalf("ApplyXPUTopologyUpdate(%s) error = %v", nodeName, err)
		}
	}

	group := &scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "train", Namespace: "default"},
		Spec: scheduling.PodGroupSpec{MinMember: 1, DeviceTopology: &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{{
			ResourceName: compilerTestResource,
			ApplyTo:      scheduling.DeviceTopologyApplyToPod,
			Mode:         scheduling.SoftDeviceTopologyMode,
			Domain: scheduling.DeviceTopologyDomainSelector{
				Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up",
			},
		}}}},
		Status: scheduling.PodGroupStatus{Phase: scheduling.PodGroupPending},
	}
	schedulerCache.AddPodGroupV1beta1(group)
	queue := &scheduling.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec:       scheduling.QueueSpec{Weight: 1},
		Status:     scheduling.QueueStatus{Allocated: corev1.ResourceList{}},
	}
	schedulerCache.AddQueueV1beta1(queue)
	pod := integrationPod("worker", "train")
	schedulerCache.AddPod(pod)
	ctx := context.Background()
	if _, err := schedulerCache.Client().CoreV1().Pods("default").Create(ctx, pod.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create fake Pod: %v", err)
	}
	if _, err := schedulerCache.VCClient().SchedulingV1beta1().PodGroups("default").Create(ctx, group.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create fake PodGroup: %v", err)
	}
	if _, err := schedulerCache.VCClient().SchedulingV1beta1().Queues().Create(ctx, queue.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create fake Queue: %v", err)
	}

	framework.RegisterPluginBuilder(PluginName, New)
	defer framework.CleanupPluginBuilders()
	enabled := true
	session := framework.OpenSession(schedulerCache, []conf.Tier{{Plugins: []conf.PluginOption{{
		Name: PluginName, EnabledNodeOrder: &enabled,
		Arguments: map[string]interface{}{ProviderArgument: MockProvider, CatalogPathArgument: "test-catalog.json"},
	}}}}, nil)
	defer framework.CloseSession(session)

	job := session.Jobs[api.JobID("default/train")]
	if job == nil {
		t.Fatal("PodGroup job was absent from Session")
	}
	if result := session.JobValid(job); result != nil {
		t.Fatalf("soft policy unexpectedly blocked: %#v", result)
	}
	task := job.Tasks[api.TaskID("worker")]
	if task == nil {
		t.Fatalf("Pod task was absent from Session: %#v", job.Tasks)
	}
	scores, err := session.BatchNodeOrderFn(task, []*api.NodeInfo{session.Nodes["node-b"], session.Nodes["node-a"]})
	if err != nil {
		t.Fatalf("BatchNodeOrderFn() error = %v", err)
	}
	if scores["node-a"] <= scores["node-b"] {
		t.Fatalf("session scorer did not prefer exact-fit node: %#v", scores)
	}
}

func integrationCatalog(t *testing.T) *topology.Catalog {
	t.Helper()
	catalog, err := topology.LoadCatalog(strings.NewReader(`{
  "resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Node","name":"local-scale-up"}]}]
}`))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	return catalog
}

func integrationNode(name string) *corev1.Node {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:   resource.MustParse("4"),
		compilerTestResource: resource.MustParse("4"),
		corev1.ResourcePods:  resource.MustParse("20"),
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), ResourceVersion: "1"},
		Status:     corev1.NodeStatus{Capacity: resources, Allocatable: resources},
	}
}

func integrationPod(name, group string) *corev1.Pod {
	quantity := resource.MustParse("1")
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID(name),
			Annotations: map[string]string{scheduling.KubeGroupNameAnnotationKey: group},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
		Spec: corev1.PodSpec{SchedulerName: "volcano", Containers: []corev1.Container{{
			Name: "worker", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{compilerTestResource: quantity}, Limits: corev1.ResourceList{compilerTestResource: quantity},
			},
		}}},
	}
}
