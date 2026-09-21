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
	"fmt"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

// xpuTopologySnapshotCache is the cache-owned bridge between the
// process-scoped Provider/Normalizer state and a SchedulerCache snapshot. Its
// nodeEpoch and nodes fields are protected by SchedulerCache.Mutex. updateMu
// serializes Provider updates and retired-UID cleanup, but is never acquired
// while SchedulerCache.Mutex is held.
type xpuTopologySnapshotCache struct {
	updateMu sync.Mutex

	published atomic.Pointer[api.DeviceTopologySnapshot]
	nodeEpoch uint64
	nodes     map[string]api.DeviceTopologyNodeState
	ingestor  *topology.FactsIngestor

	// beforeIngestorApply is test-only instrumentation proving that Provider
	// parsing/normalization runs after SchedulerCache.Mutex has been released.
	beforeIngestorApply func()
}

func newXPUTopologySnapshotCache() *xpuTopologySnapshotCache {
	state := &xpuTopologySnapshotCache{
		nodes: make(map[string]api.DeviceTopologyNodeState),
	}
	state.published.Store(api.NewDeviceTopologySnapshot(nil, nil, nil, nil, nil, nil, nil))
	return state
}

func (sc *SchedulerCache) xpuTopologySnapshotCache() *xpuTopologySnapshotCache {
	sc.xpuTopologySnapshotCacheOnce.Do(func() {
		sc.xpuTopologySnapshotState = newXPUTopologySnapshotCache()
	})
	return sc.xpuTopologySnapshotState
}

// ConfigureXPUTopologyFactsIngestor installs the single process-scoped facts
// ingestor consumed by this cache. It is intentionally a narrow, explicit
// seam: Session plugins only read the published ClusterInfo pointer and never
// start, replace, or stop a Provider.
func (sc *SchedulerCache) ConfigureXPUTopologyFactsIngestor(ingestor *topology.FactsIngestor) error {
	if ingestor == nil {
		return fmt.Errorf("xpu topology facts ingestor is required")
	}
	state := sc.xpuTopologySnapshotCache()
	state.updateMu.Lock()
	defer state.updateMu.Unlock()
	if state.ingestor != nil && state.ingestor != ingestor {
		return fmt.Errorf("xpu topology facts ingestor is process-scoped and cannot be replaced")
	}
	state.ingestor = ingestor

	// Nodes may have been populated before the process-scoped Provider is
	// configured. Seed them as Pending while holding the same cache lock used
	// for ordinary Node observations, then enable event-driven updates.
	sc.Mutex.Lock()
	for name, nodeInfo := range sc.Nodes {
		if nodeInfo == nil || nodeInfo.Node == nil {
			continue
		}
		identity := api.NodeIdentity{Name: name, UID: nodeInfo.Node.UID}
		state.nodes[name] = api.DeviceTopologyNodeState{Identity: identity, SyncState: api.DeviceTopologyNodePending}
		state.nodeEpoch++
	}
	state.published.Store(api.FilterDeviceTopologySnapshot(state.published.Load(), state.nodes))
	sc.xpuTopologySnapshotEnabled.Store(true)
	sc.Mutex.Unlock()
	return nil
}

// ApplyXPUTopologyUpdate validates and normalizes one Provider update outside
// SchedulerCache.Mutex, then publishes an immutable topology pointer only if
// the Node observation seen before parsing still exists. A late update is
// discarded together with its retired UID record rather than being allowed to
// contaminate a same-name Node replacement.
func (sc *SchedulerCache) ApplyXPUTopologyUpdate(update provider.ProviderNodeUpdate) error {
	state := sc.xpuTopologySnapshotCache()
	state.updateMu.Lock()
	defer state.updateMu.Unlock()
	if state.ingestor == nil {
		return fmt.Errorf("xpu topology facts ingestor is not configured")
	}

	// This short read is the only SchedulerCache lock acquisition before
	// parsing/normalization. FactsIngestor.Apply performs all expensive work
	// after the lock is released.
	sc.Mutex.Lock()
	identity, found := state.nodes[update.NodeName]
	epoch := state.nodeEpoch
	nodeStates := cloneDeviceTopologyNodeStates(state.nodes)
	sc.Mutex.Unlock()
	if !found {
		return nodeObservationOutOfDate("Node is absent from SchedulerCache")
	}
	if state.beforeIngestorApply != nil {
		state.beforeIngestorApply()
	}

	_, canonical, err := state.ingestor.Apply(update, identity.Identity)
	if err != nil {
		return err
	}

	candidateNodes := cloneDeviceTopologyNodeStates(nodeStates)
	candidateNodes[update.NodeName] = api.DeviceTopologyNodeState{
		Identity:         identity.Identity,
		SyncState:        api.DeviceTopologyNodeSynced,
		SourceGeneration: update.SourceGeneration,
	}
	candidate := deviceTopologySnapshotFromCanonical(canonical, candidateNodes)

	sc.Mutex.Lock()
	current, stillPresent := state.nodes[update.NodeName]
	accepted := stillPresent && state.nodeEpoch == epoch && current.Identity == identity.Identity
	if accepted {
		state.nodes = candidateNodes
		state.published.Store(candidate)
	}
	sc.Mutex.Unlock()
	if !accepted {
		// The provider update was admitted while an old Node observation was
		// current, then the Node changed before publication. Retire all records
		// for that immutable UID; a replacement UID must resync from Pending.
		if _, forgetErr := state.ingestor.ForgetNodeObservation(identity.Identity); forgetErr != nil {
			return fmt.Errorf("discard outdated xpu topology update: %w", forgetErr)
		}
		return nodeObservationOutOfDate("Node changed while topology facts were being normalized")
	}

	// Provider readiness means this process has published at least one valid
	// observation. Per-Node Pending/Synced state remains in the snapshot and is
	// deliberately not collapsed into this process-wide activation bit.
	sc.XPUTopologyManager().SetReadiness(topology.Readiness{ProviderReady: true})
	return nil
}

// observeXPUTopologyNodeLocked records a fresh Node observation and publishes
// a Pending view before callers release SchedulerCache.Mutex. It returns a
// retired Node observation for cleanup outside the main cache lock. The caller
// must hold SchedulerCache.Mutex.
func (sc *SchedulerCache) observeXPUTopologyNodeLocked(node *v1.Node) (api.NodeIdentity, bool) {
	if !sc.xpuTopologySnapshotEnabled.Load() {
		return api.NodeIdentity{}, false
	}
	state := sc.xpuTopologySnapshotCache()
	identity := api.NodeIdentity{Name: node.Name, UID: node.UID}
	current, found := state.nodes[node.Name]
	if found && current.Identity == identity {
		return api.NodeIdentity{}, false
	}

	retired := current.Identity
	state.nodeEpoch++
	state.nodes[node.Name] = api.DeviceTopologyNodeState{
		Identity:  identity,
		SyncState: api.DeviceTopologyNodePending,
	}
	state.published.Store(api.FilterDeviceTopologySnapshot(state.published.Load(), state.nodes))
	return retired, found
}

// removeXPUTopologyNodeLocked removes a Node observation and atomically
// publishes a view that no longer contains its topology facts. The caller must
// hold SchedulerCache.Mutex.
func (sc *SchedulerCache) removeXPUTopologyNodeLocked(nodeName string) (api.NodeIdentity, bool) {
	if !sc.xpuTopologySnapshotEnabled.Load() {
		return api.NodeIdentity{}, false
	}
	state := sc.xpuTopologySnapshotCache()
	current, found := state.nodes[nodeName]
	if !found {
		return api.NodeIdentity{}, false
	}
	delete(state.nodes, nodeName)
	state.nodeEpoch++
	state.published.Store(api.FilterDeviceTopologySnapshot(state.published.Load(), state.nodes))
	return current.Identity, true
}

// forgetXPUTopologyNodeObservation is deliberately invoked after the
// SchedulerCache lock has been released. It removes tracker state that can no
// longer be published; the Pending pointer was already installed by the
// caller.
func (sc *SchedulerCache) forgetXPUTopologyNodeObservation(node api.NodeIdentity, found bool) {
	if !found {
		return
	}
	state := sc.xpuTopologySnapshotCache()
	state.updateMu.Lock()
	defer state.updateMu.Unlock()
	if state.ingestor == nil {
		return
	}
	if _, err := state.ingestor.ForgetNodeObservation(node); err != nil {
		klog.ErrorS(err, "Failed to retire xPU topology facts for Node observation", "node", node.Name, "nodeUID", node.UID)
	}
}

func (sc *SchedulerCache) xpuTopologySnapshotLocked() *api.DeviceTopologySnapshot {
	return sc.xpuTopologySnapshotCache().published.Load()
}

func deviceTopologySnapshotFromCanonical(canonical topology.CanonicalTopology, nodes map[string]api.DeviceTopologyNodeState) *api.DeviceTopologySnapshot {
	pending := make([]api.PendingFabric, len(canonical.PendingFabrics))
	for index, fabric := range canonical.PendingFabrics {
		pending[index] = api.PendingFabric{Key: fabric.Key, OwnerNodeUID: fabric.OwnerNodeUID, Reason: fabric.Reason}
	}
	snapshot := api.NewDeviceTopologySnapshot(
		nodes,
		canonical.Devices,
		canonical.LocalDomains,
		canonical.Fabrics,
		canonical.LocalDomainsByClass,
		canonical.FabricsByClass,
		pending,
	)
	return api.FilterDeviceTopologySnapshot(snapshot, nodes)
}

func cloneDeviceTopologyNodeStates(source map[string]api.DeviceTopologyNodeState) map[string]api.DeviceTopologyNodeState {
	result := make(map[string]api.DeviceTopologyNodeState, len(source))
	for name, state := range source {
		result[name] = state
	}
	return result
}

func nodeObservationOutOfDate(problem string) error {
	return &topology.FactValidationError{Reason: topology.NodeObservationOutOfDate, Problem: problem}
}
