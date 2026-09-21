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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

const topologyCacheCatalog = `{
  "resources":[{
    "resourceName":"nvidia.com/gpu",
    "domainClasses":[
      {"scope":"Node","name":"local-scale-up"},
      {"scope":"Fabric","name":"scale-up-fabric"}
    ]
  }]
}`

func TestXPUTopologyPairedSnapshotRejectsNodeReplacement(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	ingestor := newTopologyCacheIngestor(t)
	if err := sc.ConfigureXPUTopologyFactsIngestor(ingestor); err != nil {
		t.Fatalf("ConfigureXPUTopologyFactsIngestor() error = %v", err)
	}

	oldNode := topologyCacheNode("node-a", "uid-a", "1")
	if err := sc.AddOrUpdateNode(oldNode); err != nil {
		t.Fatalf("AddOrUpdateNode(old) error = %v", err)
	}
	facts := topologyCacheNodeFacts("gpu-a0", "domain-a")
	oldIdentity := topologyCacheNodeIdentity(oldNode)
	if err := sc.ApplyXPUTopologyUpdate(topologyCacheMock().Replace(oldIdentity, 1, facts, time.Unix(1, 0), time.Unix(10, 0))); err != nil {
		t.Fatalf("ApplyXPUTopologyUpdate(old) error = %v", err)
	}
	oldSnapshot := sc.Snapshot().DeviceTopology
	if oldSnapshot == nil || oldSnapshot.Nodes[oldNode.Name].SyncState != api.DeviceTopologyNodeSynced || len(oldSnapshot.Devices) != 1 {
		t.Fatalf("old paired snapshot = %#v, want one synced device", oldSnapshot)
	}
	localClass := api.DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}
	if !oldSnapshot.ProviderCapabilitiesKnown {
		t.Fatalf("Provider capabilities were not published with paired snapshot")
	}
	if _, found := oldSnapshot.SupportedProviderDomainClasses[localClass]; !found {
		t.Fatalf("published Provider capabilities omit local class: %#v", oldSnapshot.SupportedProviderDomainClasses)
	}
	if got, want := oldSnapshot.Nodes[oldNode.Name].FreshUntil, time.Unix(10, 0); !got.Equal(want) {
		t.Fatalf("FreshUntil = %v, want %v", got, want)
	}

	// A same-UID Node metadata update changes ResourceVersion but not the Node
	// incarnation. Its already valid topology remains paired and usable.
	metadataUpdate := oldNode.DeepCopy()
	metadataUpdate.ResourceVersion = "2"
	metadataUpdate.Labels = map[string]string{"topology": "unchanged"}
	if err := sc.AddOrUpdateNode(metadataUpdate); err != nil {
		t.Fatalf("AddOrUpdateNode(metadata update) error = %v", err)
	}
	metadataSnapshot := sc.Snapshot().DeviceTopology
	if metadataSnapshot != oldSnapshot || metadataSnapshot.Nodes[oldNode.Name].SyncState != api.DeviceTopologyNodeSynced || len(metadataSnapshot.Devices) != 1 {
		t.Fatalf("same-UID metadata update changed topology: %#v", metadataSnapshot)
	}

	// The published snapshot must not retain aliases to caller-owned Provider
	// input after Apply returns.
	facts.Devices[0].Health = api.DeviceUnhealthy
	for _, device := range oldSnapshot.Devices {
		if device.Health != api.DeviceHealthy {
			t.Fatalf("published snapshot changed after source mutation: %#v", device)
		}
	}

	newNode := topologyCacheNode("node-a", "uid-b", "2")
	if err := sc.AddOrUpdateNode(newNode); err != nil {
		t.Fatalf("AddOrUpdateNode(replacement) error = %v", err)
	}
	pendingSnapshot := sc.Snapshot().DeviceTopology
	if state := pendingSnapshot.Nodes[newNode.Name]; state.Identity.UID != newNode.UID || state.SyncState != api.DeviceTopologyNodePending {
		t.Fatalf("replacement state = %#v, want new UID Pending", state)
	}
	if len(pendingSnapshot.Devices) != 0 || len(pendingSnapshot.LocalDomains) != 0 || len(pendingSnapshot.Fabrics) != 0 {
		t.Fatalf("replacement retained old topology: %#v", pendingSnapshot)
	}
	if oldSnapshot.Nodes[oldNode.Name].Identity.UID != oldNode.UID || oldSnapshot.Nodes[oldNode.Name].SyncState != api.DeviceTopologyNodeSynced || len(oldSnapshot.Devices) != 1 {
		t.Fatalf("old Session view was mutated by replacement: %#v", oldSnapshot)
	}

	if err := sc.ApplyXPUTopologyUpdate(topologyCacheMock().Replace(oldIdentity, 2, topologyCacheNodeFacts("gpu-a1", "domain-a"), time.Unix(2, 0), time.Unix(10, 0))); !isNodeObservationOutOfDate(err) {
		t.Fatalf("late old-UID update error = %v, want NodeObservationOutOfDate", err)
	}
	if got := sc.Snapshot().DeviceTopology; got.Nodes[newNode.Name].SyncState != api.DeviceTopologyNodePending || len(got.Devices) != 0 {
		t.Fatalf("late old-UID update changed new Node view: %#v", got)
	}

	newIdentity := topologyCacheNodeIdentity(newNode)
	if err := sc.ApplyXPUTopologyUpdate(topologyCacheMock().Replace(newIdentity, 1, topologyCacheNodeFacts("gpu-b0", "domain-b"), time.Unix(3, 0), time.Unix(10, 0))); err != nil {
		t.Fatalf("ApplyXPUTopologyUpdate(new) error = %v", err)
	}
	newSnapshot := sc.Snapshot().DeviceTopology
	if newSnapshot == oldSnapshot || newSnapshot.Nodes[newNode.Name].SyncState != api.DeviceTopologyNodeSynced || len(newSnapshot.Devices) != 1 {
		t.Fatalf("new paired snapshot = %#v, want distinct synced replacement view", newSnapshot)
	}

	clear := provider.ClearAnnotation(topologyCacheMock().Identity, "nvidia.com/gpu", newIdentity, 2, time.Unix(4, 0), time.Unix(10, 0))
	if err := sc.ApplyXPUTopologyUpdate(clear); err != nil {
		t.Fatalf("ApplyXPUTopologyUpdate(clear) error = %v", err)
	}
	clearedSnapshot := sc.Snapshot().DeviceTopology
	if clearedSnapshot == newSnapshot || clearedSnapshot.Nodes[newNode.Name].SyncState != api.DeviceTopologyNodeSynced || len(clearedSnapshot.Devices) != 0 {
		t.Fatalf("ClearFacts snapshot = %#v, want a distinct synced empty inventory", clearedSnapshot)
	}
	if len(newSnapshot.Devices) != 1 {
		t.Fatalf("old snapshot changed after ClearFacts: %#v", newSnapshot)
	}

	if err := sc.RemoveNode(newNode.Name); err != nil {
		t.Fatalf("RemoveNode() error = %v", err)
	}
	deletedSnapshot := sc.Snapshot().DeviceTopology
	if _, found := deletedSnapshot.Nodes[newNode.Name]; found || len(deletedSnapshot.Devices) != 0 {
		t.Fatalf("deleted Node retained topology: %#v", deletedSnapshot)
	}
}

func TestXPUTopologyAnnotationRefreshReplacesClearsAndPreservesFactsOnError(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	ingestor := newTopologyCacheIngestor(t)
	identity := topologyCacheMock().Identity
	if err := sc.ConfigureXPUTopologyAnnotationProvider(ingestor, identity); err != nil {
		t.Fatalf("ConfigureXPUTopologyAnnotationProvider() error = %v", err)
	}

	node := topologyCacheNode("node-a", "uid-a", "1")
	node.Annotations = map[string]string{provider.AnnotationKey: topologyCacheAnnotation(1, "gpu-a0", "domain-a")}
	if err := sc.AddOrUpdateNode(node); err != nil {
		t.Fatalf("AddOrUpdateNode(initial annotation) error = %v", err)
	}
	initial := sc.Snapshot().DeviceTopology
	if initial.Nodes[node.Name].SyncState != api.DeviceTopologyNodeSynced || len(initial.Devices) != 1 || !hasTopologyCacheDevice(initial, "gpu-a0") {
		t.Fatalf("initial annotation snapshot = %#v, want synced gpu-a0", initial)
	}

	metadataOnly := node.DeepCopy()
	metadataOnly.ResourceVersion = "2"
	if err := sc.AddOrUpdateNode(metadataOnly); err != nil {
		t.Fatalf("AddOrUpdateNode(metadata-only annotation update) error = %v", err)
	}
	if got := sc.Snapshot().DeviceTopology; got != initial {
		t.Fatalf("resourceVersion-only update republished topology: initial=%p got=%p", initial, got)
	}

	replaced := metadataOnly.DeepCopy()
	replaced.ResourceVersion = "3"
	replaced.Annotations = map[string]string{provider.AnnotationKey: topologyCacheAnnotation(2, "gpu-a1", "domain-a")}
	if err := sc.AddOrUpdateNode(replaced); err != nil {
		t.Fatalf("AddOrUpdateNode(annotation replacement) error = %v", err)
	}
	replacedSnapshot := sc.Snapshot().DeviceTopology
	if replacedSnapshot == initial || replacedSnapshot.Nodes[node.Name].SyncState != api.DeviceTopologyNodeSynced || len(replacedSnapshot.Devices) != 1 || !hasTopologyCacheDevice(replacedSnapshot, "gpu-a1") {
		t.Fatalf("annotation replacement snapshot = %#v, want synced gpu-a1", replacedSnapshot)
	}

	invalid := replaced.DeepCopy()
	invalid.ResourceVersion = "4"
	invalid.Annotations = map[string]string{provider.AnnotationKey: "{"}
	if err := sc.AddOrUpdateNode(invalid); err != nil {
		t.Fatalf("AddOrUpdateNode(invalid annotation) error = %v", err)
	}
	if got := sc.Snapshot().DeviceTopology; got != replacedSnapshot || len(got.Devices) != 1 || !hasTopologyCacheDevice(got, "gpu-a1") {
		t.Fatalf("invalid annotation replaced accepted facts: %#v", got)
	}

	cleared := invalid.DeepCopy()
	cleared.ResourceVersion = "5"
	delete(cleared.Annotations, provider.AnnotationKey)
	if err := sc.AddOrUpdateNode(cleared); err != nil {
		t.Fatalf("AddOrUpdateNode(annotation deletion) error = %v", err)
	}
	clearedSnapshot := sc.Snapshot().DeviceTopology
	if clearedSnapshot == replacedSnapshot || clearedSnapshot.Nodes[node.Name].SyncState != api.DeviceTopologyNodeSynced || len(clearedSnapshot.Devices) != 0 {
		t.Fatalf("annotation deletion snapshot = %#v, want synced empty inventory", clearedSnapshot)
	}
}

func TestXPUTopologyAnnotationProviderInitializesExistingNode(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	node := topologyCacheNode("node-a", "uid-a", "1")
	node.Annotations = map[string]string{provider.AnnotationKey: topologyCacheAnnotation(1, "gpu-a0", "domain-a")}
	if err := sc.AddOrUpdateNode(node); err != nil {
		t.Fatalf("AddOrUpdateNode() error = %v", err)
	}

	if err := sc.ConfigureXPUTopologyAnnotationProvider(newTopologyCacheIngestor(t), topologyCacheMock().Identity); err != nil {
		t.Fatalf("ConfigureXPUTopologyAnnotationProvider() error = %v", err)
	}
	snapshot := sc.Snapshot().DeviceTopology
	if snapshot.Nodes[node.Name].SyncState != api.DeviceTopologyNodeSynced || len(snapshot.Devices) != 1 || !hasTopologyCacheDevice(snapshot, "gpu-a0") {
		t.Fatalf("existing Node annotation snapshot = %#v, want synced gpu-a0", snapshot)
	}
}

func TestXPUTopologyFabricMemberReplacementInvalidatesPublishedFabric(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	if err := sc.ConfigureXPUTopologyFactsIngestor(newTopologyCacheIngestor(t)); err != nil {
		t.Fatalf("ConfigureXPUTopologyFactsIngestor() error = %v", err)
	}
	nodeA := topologyCacheNode("node-a", "uid-a", "1")
	nodeB := topologyCacheNode("node-b", "uid-b", "1")
	if err := sc.AddOrUpdateNode(nodeA); err != nil {
		t.Fatalf("AddOrUpdateNode(node-a) error = %v", err)
	}
	if err := sc.AddOrUpdateNode(nodeB); err != nil {
		t.Fatalf("AddOrUpdateNode(node-b) error = %v", err)
	}

	if err := sc.ApplyXPUTopologyUpdate(topologyCacheMock().Replace(topologyCacheNodeIdentity(nodeA), 1, topologyCacheFabricOwnerFacts(), time.Unix(1, 0), time.Unix(10, 0))); err != nil {
		t.Fatalf("ApplyXPUTopologyUpdate(owner) error = %v", err)
	}
	if err := sc.ApplyXPUTopologyUpdate(topologyCacheMock().Replace(topologyCacheNodeIdentity(nodeB), 1, topologyCacheNodeFacts("gpu-b0", "domain-b"), time.Unix(2, 0), time.Unix(10, 0))); err != nil {
		t.Fatalf("ApplyXPUTopologyUpdate(member) error = %v", err)
	}
	if got := sc.Snapshot().DeviceTopology; len(got.Fabrics) != 1 {
		t.Fatalf("complete Fabric snapshot = %#v, want one Fabric", got)
	}

	if err := sc.AddOrUpdateNode(topologyCacheNode("node-b", "uid-b-replacement", "2")); err != nil {
		t.Fatalf("AddOrUpdateNode(member replacement) error = %v", err)
	}
	got := sc.Snapshot().DeviceTopology
	if got.Nodes["node-b"].SyncState != api.DeviceTopologyNodePending || len(got.Fabrics) != 0 {
		t.Fatalf("member replacement retained Fabric: %#v", got)
	}
}

func TestXPUTopologyProviderApplyRunsOutsideMainCacheLockAndDiscardsLatePublish(t *testing.T) {
	sc := NewDefaultMockSchedulerCache("volcano")
	if err := sc.ConfigureXPUTopologyFactsIngestor(newTopologyCacheIngestor(t)); err != nil {
		t.Fatalf("ConfigureXPUTopologyFactsIngestor() error = %v", err)
	}
	oldNode := topologyCacheNode("node-a", "uid-a", "1")
	if err := sc.AddOrUpdateNode(oldNode); err != nil {
		t.Fatalf("AddOrUpdateNode(old) error = %v", err)
	}

	state := sc.xpuTopologySnapshotCache()
	entered := make(chan struct{})
	release := make(chan struct{})
	state.beforeIngestorApply = func() {
		if !sc.Mutex.TryLock() {
			t.Error("Provider apply ran while SchedulerCache.Mutex was held")
		} else {
			sc.Mutex.Unlock()
		}
		close(entered)
		<-release
	}
	applyResult := make(chan error, 1)
	go func() {
		applyResult <- sc.ApplyXPUTopologyUpdate(topologyCacheMock().Replace(topologyCacheNodeIdentity(oldNode), 1, topologyCacheNodeFacts("gpu-a0", "domain-a"), time.Unix(1, 0), time.Unix(10, 0)))
	}()
	<-entered

	addResult := make(chan error, 1)
	go func() {
		addResult <- sc.AddOrUpdateNode(topologyCacheNode("node-a", "uid-b", "2"))
	}()
	awaitTopologyNodeUID(t, sc, "node-a", "uid-b")
	close(release)
	if err := <-applyResult; !isNodeObservationOutOfDate(err) {
		t.Fatalf("late publish error = %v, want NodeObservationOutOfDate", err)
	}
	if err := <-addResult; err != nil {
		t.Fatalf("AddOrUpdateNode(replacement) error = %v", err)
	}
	if got := sc.Snapshot().DeviceTopology; got.Nodes["node-a"].SyncState != api.DeviceTopologyNodePending || len(got.Devices) != 0 {
		t.Fatalf("late publish changed replacement view: %#v", got)
	}
}

func newTopologyCacheIngestor(t *testing.T) *topology.FactsIngestor {
	t.Helper()
	catalog, err := topology.LoadCatalog(strings.NewReader(topologyCacheCatalog))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	ingestor, err := topology.NewFactsIngestor(catalog, provider.TopologyProviderCapabilities{
		Identity:     provider.ProviderIdentityRef{ProviderID: "cache-test-provider", Namespace: "test.example"},
		ResourceName: "nvidia.com/gpu",
		DomainClasses: []api.DomainClassKey{
			{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"},
			{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeFabric, Name: "scale-up-fabric"},
		},
	})
	if err != nil {
		t.Fatalf("NewFactsIngestor() error = %v", err)
	}
	return ingestor
}

func topologyCacheMock() provider.MockProvider {
	return provider.MockProvider{Identity: provider.ProviderIdentityRef{ProviderID: "cache-test-provider", Namespace: "test.example"}}
}

func topologyCacheNode(name string, uid types.UID, resourceVersion string) *v1.Node {
	resources := v1.ResourceList{
		v1.ResourceCPU:                    resource.MustParse("4"),
		v1.ResourceName("nvidia.com/gpu"): resource.MustParse("2"),
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, ResourceVersion: resourceVersion},
		Status:     v1.NodeStatus{Capacity: resources, Allocatable: resources},
	}
}

func topologyCacheNodeIdentity(node *v1.Node) api.NodeObservation {
	return api.NodeObservation{
		Identity:        api.NodeIdentity{Name: node.Name, UID: node.UID},
		ResourceVersion: node.ResourceVersion,
	}
}

func topologyCacheNodeFacts(deviceID api.SourceDeviceID, domainID api.SourceDomainID) provider.NodeTopologyFacts {
	return provider.NodeTopologyFacts{
		ResourceName: "nvidia.com/gpu",
		Devices:      []provider.DeviceFact{{ID: deviceID, Health: api.DeviceHealthy}},
		LocalDomains: []provider.LocalDomainFact{{ID: domainID, DomainClass: "local-scale-up", DeviceIDs: []api.SourceDeviceID{deviceID}}},
	}
}

func topologyCacheAnnotation(sourceGeneration uint64, deviceID, domainID string) string {
	return fmt.Sprintf(`{"resourceName":"nvidia.com/gpu","sourceGeneration":%d,"devices":[{"id":"%s","health":"Healthy"}],"localDomains":[{"id":"%s","domainClass":"local-scale-up","deviceIDs":["%s"]}]}`, sourceGeneration, deviceID, domainID, deviceID)
}

func hasTopologyCacheDevice(snapshot *api.DeviceTopologySnapshot, deviceID string) bool {
	for key := range snapshot.Devices {
		if string(key.ID.Value) == deviceID {
			return true
		}
	}
	return false
}

func topologyCacheFabricOwnerFacts() provider.NodeTopologyFacts {
	facts := topologyCacheNodeFacts("gpu-a0", "domain-a")
	facts.Fabrics = []provider.FabricFact{{
		ID:          "fabric-0",
		DomainClass: "scale-up-fabric",
		OwnerNode:   "node-a",
		Members: []provider.FabricMemberFact{
			{NodeName: "node-a", SourceGeneration: 1, LocalDomainIDs: []api.SourceDomainID{"domain-a"}},
			{NodeName: "node-b", SourceGeneration: 1, LocalDomainIDs: []api.SourceDomainID{"domain-b"}},
		},
	}}
	return facts
}

func isNodeObservationOutOfDate(err error) bool {
	var validationErr *topology.FactValidationError
	return errors.As(err, &validationErr) && validationErr.Reason == topology.NodeObservationOutOfDate
}

func awaitTopologyNodeUID(t *testing.T, sc *SchedulerCache, nodeName string, uid types.UID) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sc.Mutex.Lock()
		state, found := sc.xpuTopologySnapshotCache().nodes[nodeName]
		sc.Mutex.Unlock()
		if found && state.Identity.UID == uid {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Node %q did not reach UID %q", nodeName, uid)
}
