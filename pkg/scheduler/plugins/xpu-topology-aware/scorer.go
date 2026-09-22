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
	"fmt"
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// MaxAdvisoryScore bounds the complete xPU contribution for one Task. The
// framework adds this value to other NodeOrder plugin scores; it is not a
// filter and does not replace ordinary resource-fit accounting.
const MaxAdvisoryScore = 100.0

// placement is a structural, not device-level, choice. Exactly one of local
// and fabric is set. It is kept only in one Session's group soft overlay.
type placement struct {
	scope    scheduling.DeviceTopologyDomainScope
	local    api.LocalDomainKey
	fabric   api.FabricKey
	capacity int64
	members  int
	key      string
}

func (p placement) equal(other placement) bool {
	if p.scope != other.scope {
		return false
	}
	switch p.scope {
	case scheduling.DeviceTopologyDomainScopeNode:
		return p.local == other.local
	case scheduling.DeviceTopologyDomainScopeFabric:
		return p.fabric == other.fabric
	default:
		return false
	}
}

type scoredPlacement struct {
	nodeName string
	placement
}

// groupSoftOverlay records the first Session-local structural preference made
// for each Group policy. It is deliberately updated by Allocate/Deallocate
// events only and is discarded when the Session closes.
type groupSoftOverlay struct {
	mu         sync.RWMutex
	placements map[groupPolicyKey]map[api.TaskID]placement
}

func newGroupSoftOverlay() *groupSoftOverlay {
	return &groupSoftOverlay{placements: make(map[groupPolicyKey]map[api.TaskID]placement)}
}

func (o *groupSoftOverlay) add(key groupPolicyKey, task api.TaskID, value placement) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.placements[key] == nil {
		o.placements[key] = make(map[api.TaskID]placement)
	}
	o.placements[key][task] = value
}

func (o *groupSoftOverlay) remove(key groupPolicyKey, task api.TaskID) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	entries := o.placements[key]
	delete(entries, task)
	if len(entries) == 0 {
		delete(o.placements, key)
	}
}

func (o *groupSoftOverlay) preferred(key groupPolicyKey) (placement, bool) {
	if o == nil {
		return placement{}, false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	entries := o.placements[key]
	if len(entries) == 0 {
		return placement{}, false
	}
	tasks := make([]api.TaskID, 0, len(entries))
	for task := range entries {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i] < tasks[j] })
	return entries[tasks[0]], true
}

type sessionScorer struct {
	compiler *compiler
	overlay  *groupSoftOverlay
}

func newSessionScorer(compiler *compiler) *sessionScorer {
	return &sessionScorer{compiler: compiler, overlay: newGroupSoftOverlay()}
}

func (s *sessionScorer) score(task *api.TaskInfo, nodes []*api.NodeInfo, job *api.JobInfo) map[string]float64 {
	if s == nil || s.compiler == nil || task == nil || job == nil || len(nodes) == 0 {
		return map[string]float64{}
	}
	compiled := s.compiler.compileTask(job, task)
	if compiled.blocked != nil || len(compiled.softPolicies) == 0 || s.compiler.snapshot == nil {
		return map[string]float64{}
	}
	if s.compiler.manager == nil || !s.compiler.manager.Activation().ProviderReady {
		return map[string]float64{}
	}

	result := make(map[string]float64, len(nodes))
	weight := MaxAdvisoryScore / float64(len(compiled.softPolicies))
	for _, policy := range compiled.softPolicies {
		candidates := make([]scoredPlacement, 0, len(nodes))
		var preferred *placement
		if policy.groupKey != nil {
			if value, found := s.overlay.preferred(*policy.groupKey); found {
				preferred = &value
			}
		}
		for _, node := range nodes {
			if node == nil {
				continue
			}
			if candidate, found := s.compiler.bestPlacementForNode(policy, node.Name); found {
				candidates = append(candidates, scoredPlacement{nodeName: node.Name, placement: candidate})
			}
		}
		sortScoredPlacements(candidates, policy.request, preferred)
		for index, candidate := range candidates {
			// Candidates receive a deterministic, bounded descending preference.
			// Non-candidates remain absent (and therefore score zero), never
			// removed from the ordinary Predicate result.
			result[candidate.nodeName] += weight * float64(len(candidates)-index) / float64(len(candidates))
		}
	}
	return result
}

func (s *sessionScorer) allocated(task *api.TaskInfo, job *api.JobInfo) {
	if s == nil || s.compiler == nil || s.overlay == nil || task == nil || job == nil {
		return
	}
	compiled := s.compiler.compileTask(job, task)
	for _, policy := range compiled.softPolicies {
		if policy.groupKey == nil {
			continue
		}
		if candidate, found := s.compiler.bestPlacementForNode(policy, task.NodeName); found {
			s.overlay.add(*policy.groupKey, task.UID, candidate)
		}
	}
}

func (s *sessionScorer) deallocated(task *api.TaskInfo, job *api.JobInfo) {
	if s == nil || s.compiler == nil || s.overlay == nil || task == nil || job == nil {
		return
	}
	compiled := s.compiler.compileTask(job, task)
	for _, policy := range compiled.softPolicies {
		if policy.groupKey != nil {
			s.overlay.remove(*policy.groupKey, task.UID)
		}
	}
}

func (c *compiler) bestPlacementForNode(policy compiledPolicy, nodeName string) (placement, bool) {
	if c == nil || c.snapshot == nil || !c.nodeFresh(nodeName) {
		return placement{}, false
	}
	var candidates []placement
	switch policy.policy.DomainClass.Scope {
	case scheduling.DeviceTopologyDomainScopeNode:
		state := c.snapshot.Nodes[nodeName]
		for _, key := range c.snapshot.LocalDomainsByClass[policy.policy.DomainClass] {
			domain, found := c.snapshot.LocalDomains[key]
			if !found || domain.Class != policy.policy.DomainClass || domain.NodeName != nodeName || key.OwnerNodeUID != state.Identity.UID {
				continue
			}
			capacity := c.healthyDevices(domain.EffectiveDeviceKeys)
			if capacity < policy.request {
				continue
			}
			candidates = append(candidates, placement{
				scope:    scheduling.DeviceTopologyDomainScopeNode,
				local:    key,
				capacity: capacity,
				members:  1,
				key:      localDomainKeyString(key),
			})
		}
	case scheduling.DeviceTopologyDomainScopeFabric:
		state := c.snapshot.Nodes[nodeName]
		for _, key := range c.snapshot.FabricsByClass[policy.policy.DomainClass] {
			fabric, found := c.snapshot.Fabrics[key]
			if !found || fabric.Class != policy.policy.DomainClass || !c.fabricFresh(fabric) || !fabricContainsNode(fabric, nodeName, state.Identity.UID) {
				continue
			}
			capacity := c.fabricHealthyDevicesForNode(fabric, nodeName, state.Identity.UID)
			if capacity < policy.request {
				continue
			}
			candidates = append(candidates, placement{
				scope:    scheduling.DeviceTopologyDomainScopeFabric,
				fabric:   key,
				capacity: capacity,
				members:  len(fabric.Members),
				key:      fabricKeyString(key),
			})
		}
	}
	if len(candidates) == 0 {
		return placement{}, false
	}
	sort.Slice(candidates, func(i, j int) bool { return compactLess(candidates[i], candidates[j], policy.request, nil) })
	return candidates[0], true
}

func sortScoredPlacements(candidates []scoredPlacement, request int64, preferred *placement) {
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if compactLess(left.placement, right.placement, request, preferred) {
			return true
		}
		if compactLess(right.placement, left.placement, request, preferred) {
			return false
		}
		return left.nodeName < right.nodeName
	})
}

// compactLess gives deterministic Compact its M2 ordering: a Group overlay
// preference first, then structural exact fit, minimum fitting capacity,
// smaller domain/fabric fan-out, canonical key, and finally Node name.
func compactLess(left, right placement, request int64, preferred *placement) bool {
	if preferred != nil {
		leftPreferred, rightPreferred := left.equal(*preferred), right.equal(*preferred)
		if leftPreferred != rightPreferred {
			return leftPreferred
		}
	}
	leftExact, rightExact := left.capacity == request, right.capacity == request
	if leftExact != rightExact {
		return leftExact
	}
	if left.capacity != right.capacity {
		return left.capacity < right.capacity
	}
	if left.members != right.members {
		return left.members < right.members
	}
	return left.key < right.key
}

func (c *compiler) nodeFresh(nodeName string) bool {
	if c == nil || c.snapshot == nil {
		return false
	}
	state, found := c.snapshot.Nodes[nodeName]
	if !found || state.SyncState != api.DeviceTopologyNodeSynced {
		return false
	}
	return state.FreshUntil.IsZero() || c.now().Before(state.FreshUntil)
}

func (c *compiler) fabricFresh(fabric api.FabricDomain) bool {
	for _, member := range fabric.Members {
		if !c.nodeFresh(member.NodeName) {
			return false
		}
	}
	return true
}

func fabricContainsNode(fabric api.FabricDomain, nodeName string, nodeUID types.UID) bool {
	for _, member := range fabric.Members {
		if member.NodeName == nodeName && member.NodeUID == nodeUID {
			return true
		}
	}
	return false
}

func (c *compiler) healthyDevices(keys []api.DeviceKey) int64 {
	seen := make(map[api.DeviceKey]struct{}, len(keys))
	var count int64
	for _, key := range keys {
		if _, duplicated := seen[key]; duplicated {
			continue
		}
		seen[key] = struct{}{}
		device, found := c.snapshot.Devices[key]
		if !found || device.Health != api.DeviceHealthy || !c.nodeFresh(device.NodeName) {
			continue
		}
		count++
	}
	return count
}

func (c *compiler) fabricHealthyDevicesForNode(fabric api.FabricDomain, nodeName string, nodeUID types.UID) int64 {
	keys := make([]api.DeviceKey, 0)
	for _, member := range fabric.Members {
		if member.NodeName != nodeName || member.NodeUID != nodeUID {
			continue
		}
		for _, domainKey := range member.LocalDomainKeys {
			if domainKey.ResourceName != fabric.Key.ResourceName || domainKey.OwnerNodeUID != nodeUID {
				continue
			}
			domain, found := c.snapshot.LocalDomains[domainKey]
			if !found || domain.NodeName != nodeName || domain.Key.OwnerNodeUID != nodeUID {
				continue
			}
			keys = append(keys, domain.EffectiveDeviceKeys...)
		}
	}
	return c.healthyDevices(keys)
}

func localDomainKeyString(key api.LocalDomainKey) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", key.ResourceName, key.OwnerNodeUID, key.ID.ProviderID, key.ID.Namespace, key.ID.Value)
}

func fabricKeyString(key api.FabricKey) string {
	return fmt.Sprintf("%s/%s/%s/%s", key.ResourceName, key.ProviderID, key.Namespace, key.Value)
}
