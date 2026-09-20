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

package topology

import (
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

const normalizerCatalog = `{
  "resources":[{
    "resourceName":"nvidia.com/gpu",
    "domainClasses":[
      {"scope":"Node","name":"local-scale-up"},
      {"scope":"Fabric","name":"scale-up-fabric"}
    ]
  }]
}`

func TestFactsIngestorNormalizesDomainsAndWaitsForCompleteFabric(t *testing.T) {
	ingestor := newTestIngestor(t)
	nodeA := api.NodeIdentity{Name: "node-a", UID: "uid-a", ResourceVersion: "1"}
	nodeB := api.NodeIdentity{Name: "node-b", UID: "uid-b", ResourceVersion: "1"}
	mock := provider.MockProvider{Identity: testProviderIdentity()}
	factsA := nodeAFacts(1, 1)
	if got := factsA.Fabrics[0].Members[0].NodeName; got != "node-b" {
		t.Fatalf("fixture member NodeName = %q, want node-b", got)
	}
	updateA := mock.Replace(nodeA, 1, factsA, time.Unix(1, 0), time.Unix(100, 0))
	if got := updateA.Facts.Fabrics[0].Members[0].NodeName; got != "node-b" {
		t.Fatalf("provider update member NodeName = %q, want node-b", got)
	}

	_, beforeMembers, err := ingestor.Apply(updateA, nodeA)
	if err != nil {
		t.Fatalf("Apply(node-a) error = %v", err)
	}
	if len(beforeMembers.Fabrics) != 0 || len(beforeMembers.PendingFabrics) != 1 {
		t.Fatalf("topology before member facts = %#v, want one pending fabric", beforeMembers)
	}
	changedSameGeneration := nodeAFacts(1, 1)
	changedSameGeneration.Fabrics[0].Members[0].LocalDomainIDs = []api.SourceDomainID{"different-domain"}
	if _, _, err := ingestor.Apply(mock.Replace(nodeA, 1, changedSameGeneration, time.Unix(2, 0), time.Unix(100, 0)), nodeA); err == nil {
		t.Fatal("same generation with changed Fabric membership unexpectedly succeeded")
	}

	_, topology, err := ingestor.Apply(mock.Replace(nodeB, 1, nodeBFacts(), time.Unix(3, 0), time.Unix(100, 0)), nodeB)
	if err != nil {
		t.Fatalf("Apply(node-b) error = %v", err)
	}
	if len(topology.Devices) != 3 || len(topology.LocalDomains) != 3 || len(topology.Fabrics) != 1 || len(topology.PendingFabrics) != 0 {
		t.Fatalf("normalized topology sizes = devices:%d domains:%d fabrics:%d pending:%d", len(topology.Devices), len(topology.LocalDomains), len(topology.Fabrics), len(topology.PendingFabrics))
	}

	aRootKey := localDomainKey("uid-a", "a-root")
	aRoot, found := topology.LocalDomains[aRootKey]
	if !found || len(aRoot.MemberDomainKeys) != 1 || len(aRoot.EffectiveDeviceKeys) != 2 {
		t.Fatalf("a-root = %#v, want one child and two effective devices", aRoot)
	}
	if aRoot.Class != (api.DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}) {
		t.Fatalf("a-root class = %#v", aRoot.Class)
	}
	for _, device := range topology.Devices {
		if device.Key.OwnerNodeUID == "uid-a" && device.Key.ID.Value == "GPU-a1" && len(device.LocalDomainKeys) != 1 {
			t.Fatalf("GPU-a1 local membership = %#v, want direct leaf membership", device.LocalDomainKeys)
		}
	}

	fabricKey := api.FabricKey{ProviderID: "annotation-v1", Namespace: "example.test", ResourceName: "nvidia.com/gpu", Value: "fabric-0"}
	fabric, found := topology.Fabrics[fabricKey]
	if !found || len(fabric.Members) != 2 || fabric.Members[0].NodeUID != "uid-a" || fabric.Members[1].NodeUID != "uid-b" {
		t.Fatalf("fabric = %#v, want resolved sorted members", fabric)
	}

	// A member update at a new generation immediately makes the old owner
	// declaration unavailable. It cannot silently inherit membership.
	_, topology, err = ingestor.Apply(mock.Replace(nodeB, 2, nodeBFacts(), time.Unix(4, 0), time.Unix(100, 0)), nodeB)
	if err != nil {
		t.Fatalf("Apply(node-b generation 2) error = %v", err)
	}
	if len(topology.Fabrics) != 0 || len(topology.PendingFabrics) != 1 {
		t.Fatalf("fabric after member generation change = %#v, want pending", topology)
	}

	_, topology, err = ingestor.Apply(mock.Replace(nodeA, 2, nodeAFacts(2, 2), time.Unix(5, 0), time.Unix(100, 0)), nodeA)
	if err != nil {
		t.Fatalf("Apply(node-a generation 2) error = %v", err)
	}
	if len(topology.Fabrics) != 1 || len(topology.PendingFabrics) != 0 {
		t.Fatalf("fabric after complete owner replacement = %#v, want resolved", topology)
	}
}

func TestFactsIngestorRejectsInvalidAndOutdatedUpdatesWithoutReplacingFacts(t *testing.T) {
	ingestor := newTestIngestor(t)
	node := api.NodeIdentity{Name: "node-a", UID: "uid-a", ResourceVersion: "1"}
	mock := provider.MockProvider{Identity: testProviderIdentity()}
	valid := mock.Replace(node, 1, provider.NodeTopologyFacts{
		ResourceName: "nvidia.com/gpu",
		Devices: []provider.DeviceFact{
			{ID: "GPU-1", Health: api.DeviceHealthy},
			{ID: "GPU-0", Health: api.DeviceHealthy},
		},
		LocalDomains: []provider.LocalDomainFact{{ID: "local-0", DomainClass: "local-scale-up", DeviceIDs: []api.SourceDeviceID{"GPU-0", "GPU-1"}}},
	}, time.Time{}, time.Time{})
	if _, _, err := ingestor.Apply(valid, node); err != nil {
		t.Fatalf("Apply(valid) error = %v", err)
	}

	// Reordering an otherwise identical payload at the same generation is a
	// legal heartbeat because Normalizer fingerprints canonical stable order.
	reordered := valid
	reordered.Facts.Devices[0], reordered.Facts.Devices[1] = reordered.Facts.Devices[1], reordered.Facts.Devices[0]
	reordered.FreshUntil = time.Unix(30, 0)
	if _, _, err := ingestor.Apply(reordered, node); err != nil {
		t.Fatalf("Apply(reordered heartbeat) error = %v", err)
	}

	invalid := mock.Replace(node, 2, provider.NodeTopologyFacts{
		ResourceName: "nvidia.com/gpu",
		Devices: []provider.DeviceFact{
			{ID: "duplicate", Health: api.DeviceHealthy},
			{ID: "duplicate", Health: api.DeviceHealthy},
		},
	}, time.Time{}, time.Time{})
	if _, _, err := ingestor.Apply(invalid, node); err == nil {
		t.Fatal("Apply(duplicate source device) unexpectedly succeeded")
	} else {
		var validationErr *FactValidationError
		if !errors.As(err, &validationErr) || validationErr.Reason != InvalidProviderFacts {
			t.Fatalf("invalid error = %v, want structured invalid-provider error", err)
		}
	}
	record, found := ingestor.Record(valid.Key())
	if !found || record.Update.SourceGeneration != 1 || record.Update.Facts.Devices[0].ID != "GPU-0" {
		t.Fatalf("invalid update replaced valid facts: %#v", record)
	}

	outdated := valid
	outdated.NodeResourceVersion = "old"
	if _, _, err := ingestor.Apply(outdated, api.NodeIdentity{Name: node.Name, UID: node.UID, ResourceVersion: "new"}); err == nil {
		t.Fatal("outdated Node observation unexpectedly succeeded")
	} else {
		var validationErr *FactValidationError
		if !errors.As(err, &validationErr) || validationErr.Reason != NodeObservationOutOfDate {
			t.Fatalf("outdated error = %v, want NodeObservationOutOfDate", err)
		}
	}
}

func TestNormalizerRejectsCyclesAndUnsupportedClass(t *testing.T) {
	normalizer, err := NewNormalizer(loadNormalizerCatalog(t), testCapabilities())
	if err != nil {
		t.Fatalf("NewNormalizer() error = %v", err)
	}
	node := api.NodeIdentity{Name: "node-a", UID: "uid-a", ResourceVersion: "1"}
	mock := provider.MockProvider{Identity: testProviderIdentity()}
	for _, tt := range []struct {
		name  string
		facts provider.NodeTopologyFacts
		want  FactValidationReason
	}{
		{
			name: "local domain cycle",
			facts: provider.NodeTopologyFacts{ResourceName: "nvidia.com/gpu", Devices: []provider.DeviceFact{{ID: "GPU-0", Health: api.DeviceHealthy}}, LocalDomains: []provider.LocalDomainFact{
				{ID: "a", DomainClass: "local-scale-up", DomainIDs: []api.SourceDomainID{"b"}},
				{ID: "b", DomainClass: "local-scale-up", DomainIDs: []api.SourceDomainID{"a"}},
			}},
			want: InvalidProviderFacts,
		},
		{
			name: "catalog class not declared by provider",
			facts: provider.NodeTopologyFacts{ResourceName: "nvidia.com/gpu", Devices: []provider.DeviceFact{{ID: "GPU-0", Health: api.DeviceHealthy}}, LocalDomains: []provider.LocalDomainFact{
				{ID: "a", DomainClass: "not-in-catalog", DeviceIDs: []api.SourceDeviceID{"GPU-0"}},
			}},
			want: DomainClassUnsupported,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := normalizer.FingerprintUpdate(mock.Replace(node, 1, tt.facts, time.Time{}, time.Time{}))
			var validationErr *FactValidationError
			if !errors.As(err, &validationErr) || validationErr.Reason != tt.want {
				t.Fatalf("FingerprintUpdate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func newTestIngestor(t *testing.T) *FactsIngestor {
	t.Helper()
	ingestor, err := NewFactsIngestor(loadNormalizerCatalog(t), testCapabilities())
	if err != nil {
		t.Fatalf("NewFactsIngestor() error = %v", err)
	}
	return ingestor
}

func loadNormalizerCatalog(t *testing.T) *Catalog {
	t.Helper()
	catalog, err := LoadCatalog(strings.NewReader(normalizerCatalog))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	return catalog
}

func testProviderIdentity() provider.ProviderIdentityRef {
	return provider.ProviderIdentityRef{ProviderID: "annotation-v1", Namespace: "example.test"}
}

func testCapabilities() provider.TopologyProviderCapabilities {
	return provider.TopologyProviderCapabilities{
		Identity:     testProviderIdentity(),
		ResourceName: corev1.ResourceName("nvidia.com/gpu"),
		DomainClasses: []api.DomainClassKey{
			{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"},
			{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeFabric, Name: "scale-up-fabric"},
		},
	}
}

func nodeAFacts(ownerGeneration, memberGeneration uint64) provider.NodeTopologyFacts {
	return provider.NodeTopologyFacts{
		ResourceName: "nvidia.com/gpu",
		Devices: []provider.DeviceFact{
			{ID: "GPU-a1", Health: api.DeviceHealthy},
			{ID: "GPU-a0", Health: api.DeviceHealthy},
		},
		LocalDomains: []provider.LocalDomainFact{
			{ID: "a-root", DomainClass: "local-scale-up", DeviceIDs: []api.SourceDeviceID{"GPU-a0"}, DomainIDs: []api.SourceDomainID{"a-leaf"}},
			{ID: "a-leaf", DomainClass: "local-scale-up", DeviceIDs: []api.SourceDeviceID{"GPU-a1"}},
		},
		Fabrics: []provider.FabricFact{{
			ID: "fabric-0", DomainClass: "scale-up-fabric", OwnerNode: "node-a",
			Members: []provider.FabricMemberFact{
				{NodeName: "node-b", SourceGeneration: memberGeneration, LocalDomainIDs: []api.SourceDomainID{"b-local"}},
				{NodeName: "node-a", SourceGeneration: ownerGeneration, LocalDomainIDs: []api.SourceDomainID{"a-root"}},
			},
		}},
	}
}

func nodeBFacts() provider.NodeTopologyFacts {
	return provider.NodeTopologyFacts{
		ResourceName: "nvidia.com/gpu",
		Devices:      []provider.DeviceFact{{ID: "GPU-b0", Health: api.DeviceHealthy}},
		LocalDomains: []provider.LocalDomainFact{{ID: "b-local", DomainClass: "local-scale-up", DeviceIDs: []api.SourceDeviceID{"GPU-b0"}}},
	}
}

func localDomainKey(uid, id string) api.LocalDomainKey {
	return api.LocalDomainKey{
		ResourceName: "nvidia.com/gpu", OwnerNodeUID: types.UID(uid),
		ID: api.DomainID{ProviderID: "annotation-v1", Namespace: "example.test", Value: api.SourceDomainID(id)},
	}
}
