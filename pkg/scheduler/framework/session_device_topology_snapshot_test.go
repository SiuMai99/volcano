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

package framework

import (
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/topology"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

func TestSessionKeepsPairedDeviceTopologySnapshot(t *testing.T) {
	sc := cache.NewDefaultMockSchedulerCache("volcano")
	ingestor := sessionTopologyIngestor(t)
	if err := sc.ConfigureXPUTopologyFactsIngestor(ingestor); err != nil {
		t.Fatalf("ConfigureXPUTopologyFactsIngestor() error = %v", err)
	}
	oldNode := sessionTopologyNode("node-a", "uid-a", "1")
	if err := sc.AddOrUpdateNode(oldNode); err != nil {
		t.Fatalf("AddOrUpdateNode(old) error = %v", err)
	}
	if err := sc.ApplyXPUTopologyUpdate(sessionTopologyMock().Replace(sessionTopologyIdentity(oldNode), 1, sessionTopologyFacts("gpu-a0", "domain-a"), time.Unix(1, 0), time.Unix(10, 0))); err != nil {
		t.Fatalf("ApplyXPUTopologyUpdate(old) error = %v", err)
	}

	first := OpenSession(sc, nil, nil)
	defer CloseSession(first)
	if first.DeviceTopology == nil || len(first.DeviceTopology.Devices) != 1 || first.DeviceTopology.Nodes[oldNode.Name].Identity.UID != oldNode.UID {
		t.Fatalf("first Session topology = %#v", first.DeviceTopology)
	}

	newNode := sessionTopologyNode("node-a", "uid-b", "2")
	if err := sc.AddOrUpdateNode(newNode); err != nil {
		t.Fatalf("AddOrUpdateNode(replacement) error = %v", err)
	}
	second := OpenSession(sc, nil, nil)
	defer CloseSession(second)
	if second.DeviceTopology == first.DeviceTopology || len(second.DeviceTopology.Devices) != 0 || second.DeviceTopology.Nodes[newNode.Name].SyncState != api.DeviceTopologyNodePending {
		t.Fatalf("second Session topology = %#v, want new Pending pointer", second.DeviceTopology)
	}
	if len(first.DeviceTopology.Devices) != 1 || first.DeviceTopology.Nodes[oldNode.Name].Identity.UID != oldNode.UID {
		t.Fatalf("first Session topology changed after replacement: %#v", first.DeviceTopology)
	}
}

func sessionTopologyIngestor(t *testing.T) *topology.FactsIngestor {
	t.Helper()
	catalog, err := topology.LoadCatalog(strings.NewReader(`{
  "resources":[{
    "resourceName":"nvidia.com/gpu",
    "domainClasses":[{"scope":"Node","name":"local-scale-up"}]
  }]
}`))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	ingestor, err := topology.NewFactsIngestor(catalog, provider.TopologyProviderCapabilities{
		Identity:     provider.ProviderIdentityRef{ProviderID: "framework-test-provider", Namespace: "test.example"},
		ResourceName: "nvidia.com/gpu",
		DomainClasses: []api.DomainClassKey{{
			ResourceName: "nvidia.com/gpu",
			Scope:        scheduling.DeviceTopologyDomainScopeNode,
			Name:         "local-scale-up",
		}},
	})
	if err != nil {
		t.Fatalf("NewFactsIngestor() error = %v", err)
	}
	return ingestor
}

func sessionTopologyMock() provider.MockProvider {
	return provider.MockProvider{Identity: provider.ProviderIdentityRef{ProviderID: "framework-test-provider", Namespace: "test.example"}}
}

func sessionTopologyNode(name string, uid types.UID, resourceVersion string) *v1.Node {
	resources := v1.ResourceList{
		v1.ResourceCPU:                    resource.MustParse("4"),
		v1.ResourceName("nvidia.com/gpu"): resource.MustParse("2"),
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, ResourceVersion: resourceVersion},
		Status:     v1.NodeStatus{Capacity: resources, Allocatable: resources},
	}
}

func sessionTopologyIdentity(node *v1.Node) api.NodeObservation {
	return api.NodeObservation{
		Identity:        api.NodeIdentity{Name: node.Name, UID: node.UID},
		ResourceVersion: node.ResourceVersion,
	}
}

func sessionTopologyFacts(deviceID api.SourceDeviceID, domainID api.SourceDomainID) provider.NodeTopologyFacts {
	return provider.NodeTopologyFacts{
		ResourceName: "nvidia.com/gpu",
		Devices:      []provider.DeviceFact{{ID: deviceID, Health: api.DeviceHealthy}},
		LocalDomains: []provider.LocalDomainFact{{ID: domainID, DomainClass: "local-scale-up", DeviceIDs: []api.SourceDeviceID{deviceID}}},
	}
}
