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
	"encoding/json"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

type hardStep struct {
	taskIndex int
	request   api.XPUResourceRequest
	options   [][]hardPolicyOption
}

type hardPolicyOption struct {
	placement HardPolicyPlacement
	groupKey  *hardGroupPolicyKey
	instance  hardInstance
	keys      []api.DeviceKey
	capacity  int
	fanout    int
	fragments int
	key       string
}

type hardSearch struct {
	input     HardPlanInput
	steps     []hardStep
	plan      HardPlan
	used      map[api.DeviceKey]struct{}
	chosen    map[hardGroupPolicyKey]hardInstance
	states    int
	maxStates int
	exhausted bool
}

// PlanHardGroup plans every target resource of a complete admission wave on
// its already selected Node. All maps and slices in input remain untouched.
// The returned keys are tentative: an exact-capable Provider and the final
// Bind guard must confirm them after the winning Statement is known.
func PlanHardGroup(input HardPlanInput) (HardPlan, *HardPlanFailure) {
	if input.Snapshot == nil {
		return HardPlan{}, hardFailure(api.XPUTopologyDataNotReadyReason, "paired device topology snapshot is unavailable")
	}
	if input.Now.IsZero() || input.MaxStates < 0 {
		return HardPlan{}, hardFailure(hardPlanInvalidReason, "planner requires an observation time and a non-negative search budget")
	}
	maxStates := input.MaxStates
	if maxStates == 0 {
		maxStates = hardPlanDefaultMaxStates
	}
	search := &hardSearch{
		input:     input,
		used:      make(map[api.DeviceKey]struct{}, len(input.Occupied)),
		chosen:    make(map[hardGroupPolicyKey]hardInstance, len(input.Anchors)),
		maxStates: maxStates,
	}
	for _, key := range input.Occupied {
		search.used[key] = struct{}{}
	}
	for _, anchor := range input.Anchors {
		key := hardGroupPolicyKey{Group: anchor.Group, Class: anchor.Class}
		instance := hardInstance{local: copyLocal(anchor.LocalDomain), fabric: copyFabric(anchor.Fabric)}
		if anchor.Group.Job == "" || !validInstance(anchor.Class.Scope, instance) {
			return HardPlan{}, hardFailure(hardPlanInvalidReason, "invalid anchor for group %s class %v", anchor.Group.Job, anchor.Class)
		}
		if anchor.LocalDomain != nil && (anchor.LocalDomain.ResourceName != anchor.Class.ResourceName || anchor.LocalDomain.OwnerNodeUID == "") {
			return HardPlan{}, hardFailure(hardPlanInvalidReason, "local anchor identity does not match class %v", anchor.Class)
		}
		if anchor.Fabric != nil && anchor.Fabric.ResourceName != anchor.Class.ResourceName {
			return HardPlan{}, hardFailure(hardPlanInvalidReason, "Fabric anchor identity does not match class %v", anchor.Class)
		}
		if prior, exists := search.chosen[key]; exists && !sameInstance(prior, instance) {
			return HardPlan{}, hardFailure(hardPlanInvalidReason, "conflicting anchors for group %s class %v", anchor.Group.Job, anchor.Class)
		}
		search.chosen[key] = instance
	}

	sorted := append([]HardPlanTask(nil), input.Tasks...)
	for _, item := range sorted {
		if item.Task == nil || item.Task.Pod == nil || item.Task.UID == "" || item.Task.Pod.UID == "" || item.NodeName == "" {
			return HardPlan{}, hardFailure(hardPlanInvalidReason, "admission Task must have Pod UID and fixed Node")
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Task.UID < sorted[j].Task.UID
	})
	seenTasks := make(map[api.TaskID]struct{}, len(sorted))
	search.plan.Tasks = make([]HardTaskPlan, len(sorted))
	for taskIndex, item := range sorted {
		if _, exists := seenTasks[item.Task.UID]; exists {
			return HardPlan{}, hardFailure(hardPlanInvalidReason, "duplicate admission Task %s", item.Task.UID)
		}
		seenTasks[item.Task.UID] = struct{}{}
		if failure := search.nodeReady(item.NodeName); failure != nil {
			return HardPlan{}, failure
		}
		search.plan.Tasks[taskIndex] = HardTaskPlan{TaskID: item.Task.UID, PodUID: item.Task.Pod.UID, NodeName: item.NodeName}
		perResource := make(map[corev1.ResourceName][]HardPlanPolicy)
		for _, policy := range item.Policies {
			p := policy.Policy
			if p.Mode != scheduling.HardDeviceTopologyMode || p.ResourceName == "" || p.DomainClass.ResourceName != p.ResourceName || p.DomainClass.Name == "" ||
				(p.DomainClass.Scope != scheduling.DeviceTopologyDomainScopeNode && p.DomainClass.Scope != scheduling.DeviceTopologyDomainScopeFabric) {
				return HardPlan{}, hardFailure(hardPlanInvalidReason, "Task %s has an invalid hard policy", item.Task.UID)
			}
			if !policyAppliesToTask(p, item.Task) {
				continue
			}
			if p.ApplyTo != scheduling.DeviceTopologyApplyToPod && p.ApplyTo != scheduling.DeviceTopologyApplyToGroup {
				return HardPlan{}, hardFailure(hardPlanInvalidReason, "Task %s has invalid applyTo %q", item.Task.UID, p.ApplyTo)
			}
			if p.ApplyTo == scheduling.DeviceTopologyApplyToGroup && policy.Group.Job == "" {
				return HardPlan{}, hardFailure(hardPlanInvalidReason, "Task %s has a Group policy without GroupRef", item.Task.UID)
			}
			if p.ApplyTo == scheduling.DeviceTopologyApplyToGroup && policy.Group.Job != item.Task.Job {
				return HardPlan{}, hardFailure(hardPlanInvalidReason, "Task %s GroupRef does not match its Job", item.Task.UID)
			}
			perResource[p.ResourceName] = append(perResource[p.ResourceName], policy)
		}
		resources := make([]corev1.ResourceName, 0, len(perResource))
		for resourceName := range perResource {
			resources = append(resources, resourceName)
		}
		sort.Slice(resources, func(i, j int) bool { return resources[i] < resources[j] })
		for _, resourceName := range resources {
			request, err := taskResourceRequest(item.Task, resourceName)
			if err != nil {
				return HardPlan{}, hardFailure(api.XPUTopologyUnsupportedPodRequestReason, "Task %s resource %s: %v", item.Task.UID, resourceName, err)
			}
			policies := perResource[resourceName]
			sort.Slice(policies, func(i, j int) bool {
				left, right := policies[i], policies[j]
				if hardPolicySortKey(left.Policy) != hardPolicySortKey(right.Policy) {
					return hardPolicySortKey(left.Policy) < hardPolicySortKey(right.Policy)
				}
				if left.Group.Job != right.Group.Job {
					return left.Group.Job < right.Group.Job
				}
				return left.Group.SubJob < right.Group.SubJob
			})
			step := hardStep{taskIndex: taskIndex, request: request}
			for _, policy := range policies {
				if input.Snapshot.ProviderCapabilitiesKnown {
					if _, supported := input.Snapshot.SupportedProviderDomainClasses[policy.Policy.DomainClass]; !supported {
						return HardPlan{}, hardFailure(api.XPUTopologyDomainClassUnsupportedReason, "class %v is not declared by the Provider", policy.Policy.DomainClass)
					}
				}
				options, dataIssue := search.policyOptions(item.NodeName, policy, request.Count)
				if search.exhausted {
					return HardPlan{}, hardFailure(hardPlanUnavailableReason, "hard planner search budget exhausted")
				}
				if len(options) == 0 {
					if dataIssue {
						return HardPlan{}, hardFailure(api.XPUTopologyDataNotReadyReason, "Task %s has incomplete facts for class %v on Node %s", item.Task.UID, policy.Policy.DomainClass, item.NodeName)
					}
					return HardPlan{}, hardFailure(hardPlanUnsatisfiableReason, "Task %s has no matching instance for class %v on Node %s", item.Task.UID, policy.Policy.DomainClass, item.NodeName)
				}
				step.options = append(step.options, options)
			}
			search.steps = append(search.steps, step)
		}
	}
	// Constrained resources first; count ties favor larger requests, then the
	// stable Task and resource order established above.
	sort.SliceStable(search.steps, func(i, j int) bool {
		left, right := search.steps[i], search.steps[j]
		leftCount, rightCount := optionCount(left), optionCount(right)
		if leftCount != rightCount {
			return leftCount < rightCount
		}
		if left.request.Count != right.request.Count {
			return left.request.Count > right.request.Count
		}
		return false
	})
	if !search.solve(0) {
		if search.exhausted {
			return HardPlan{}, hardFailure(hardPlanUnavailableReason, "hard planner search budget exhausted")
		}
		return HardPlan{}, hardFailure(hardPlanUnsatisfiableReason, "no complete hard topology plan for the admission set")
	}
	for index := range search.plan.Tasks {
		sort.Slice(search.plan.Tasks[index].Resources, func(i, j int) bool {
			return search.plan.Tasks[index].Resources[i].Request.ResourceName < search.plan.Tasks[index].Resources[j].Request.ResourceName
		})
	}
	for key, instance := range search.chosen {
		search.plan.Anchors = append(search.plan.Anchors, HardGroupAnchor{Group: key.Group, Class: key.Class, LocalDomain: copyLocal(instance.local), Fabric: copyFabric(instance.fabric)})
	}
	sort.Slice(search.plan.Anchors, func(i, j int) bool {
		left, right := search.plan.Anchors[i], search.plan.Anchors[j]
		if left.Group.Job != right.Group.Job {
			return left.Group.Job < right.Group.Job
		}
		if left.Group.SubJob != right.Group.SubJob {
			return left.Group.SubJob < right.Group.SubJob
		}
		if left.Class.ResourceName != right.Class.ResourceName {
			return left.Class.ResourceName < right.Class.ResourceName
		}
		if left.Class.Scope != right.Class.Scope {
			return left.Class.Scope < right.Class.Scope
		}
		return left.Class.Name < right.Class.Name
	})
	return search.plan, nil
}

func (s *hardSearch) solve(stepIndex int) bool {
	if !s.tick() {
		return false
	}
	if stepIndex == len(s.steps) {
		return true
	}
	return s.choosePolicy(stepIndex, 0, nil, nil)
}

func (s *hardSearch) choosePolicy(stepIndex, policyIndex int, allowed []api.DeviceKey, placements []HardPolicyPlacement) bool {
	if !s.tick() {
		return false
	}
	step := s.steps[stepIndex]
	if policyIndex == len(step.options) {
		available := make([]api.DeviceKey, 0, len(allowed))
		for _, key := range allowed {
			if _, occupied := s.used[key]; !occupied {
				available = append(available, key)
			}
		}
		if int64(len(available)) < step.request.Count {
			return false
		}
		selected := make([]api.DeviceKey, 0, step.request.Count)
		var chooseKeys func(start int) bool
		chooseKeys = func(start int) bool {
			if !s.tick() {
				return false
			}
			if int64(len(selected)) == step.request.Count {
				for _, key := range selected {
					s.used[key] = struct{}{}
				}
				resource := HardResourcePlan{Request: step.request, DeviceKeys: append([]api.DeviceKey(nil), selected...), Placements: append([]HardPolicyPlacement(nil), placements...)}
				task := &s.plan.Tasks[step.taskIndex]
				task.Resources = append(task.Resources, resource)
				if s.solve(stepIndex + 1) {
					return true
				}
				task.Resources = task.Resources[:len(task.Resources)-1]
				for _, key := range selected {
					delete(s.used, key)
				}
				return false
			}
			need := int(step.request.Count) - len(selected)
			for index := start; index <= len(available)-need; index++ {
				selected = append(selected, available[index])
				if chooseKeys(index + 1) {
					return true
				}
				selected = selected[:len(selected)-1]
				if s.exhausted {
					return false
				}
			}
			return false
		}
		return chooseKeys(0)
	}
	for _, option := range step.options[policyIndex] {
		if !s.tick() {
			return false
		}
		if option.groupKey != nil {
			if existing, found := s.chosen[*option.groupKey]; found && !sameInstance(existing, option.instance) {
				continue
			}
		}
		next := option.keys
		if policyIndex != 0 {
			next = intersectKeys(allowed, option.keys)
		}
		if int64(len(next)) < step.request.Count {
			continue
		}
		added := false
		if option.groupKey != nil {
			if _, found := s.chosen[*option.groupKey]; !found {
				s.chosen[*option.groupKey] = option.instance
				added = true
			}
		}
		nextPlacements := append(append([]HardPolicyPlacement(nil), placements...), option.placement)
		if s.choosePolicy(stepIndex, policyIndex+1, next, nextPlacements) {
			return true
		}
		if added {
			delete(s.chosen, *option.groupKey)
		}
		if s.exhausted {
			return false
		}
	}
	return false
}

func (s *hardSearch) tick() bool {
	if s.states >= s.maxStates {
		s.exhausted = true
		return false
	}
	s.states++
	return true
}

func (s *hardSearch) nodeReady(nodeName string) *HardPlanFailure {
	state, found := s.input.Snapshot.Nodes[nodeName]
	if !found || state.Identity.Name != nodeName || state.Identity.UID == "" || state.SyncState != api.DeviceTopologyNodeSynced || state.SourceGeneration == 0 {
		return hardFailure(api.XPUTopologyDataNotReadyReason, "Node %s has no synced topology observation", nodeName)
	}
	if !state.FreshUntil.IsZero() && !s.input.Now.Before(state.FreshUntil) {
		return hardFailure(api.XPUTopologyStaleReason, "Node %s topology observation is stale", nodeName)
	}
	return nil
}

func (s *hardSearch) policyOptions(nodeName string, policy HardPlanPolicy, request int64) ([]hardPolicyOption, bool) {
	snapshot := s.input.Snapshot
	class := policy.Policy.DomainClass
	node := snapshot.Nodes[nodeName]
	var options []hardPolicyOption
	dataIssue := false
	appendOption := func(instance hardInstance, keys []api.DeviceKey, fanout, fragments int, key string) {
		if !s.tick() {
			return
		}
		keys = s.usableKeys(nodeName, class, instance, keys)
		if int64(len(keys)) < request {
			return
		}
		placement := HardPolicyPlacement{Class: class, LocalDomain: copyLocal(instance.local), Fabric: copyFabric(instance.fabric)}
		var groupKey *hardGroupPolicyKey
		if policy.Policy.ApplyTo == scheduling.DeviceTopologyApplyToGroup {
			group := policy.Group
			placement.Group = &group
			groupKey = &hardGroupPolicyKey{Group: group, Class: class}
			if anchor, found := s.chosen[*groupKey]; found && !sameInstance(anchor, instance) {
				return
			}
		}
		options = append(options, hardPolicyOption{placement: placement, groupKey: groupKey, instance: instance, keys: keys, capacity: len(keys), fanout: fanout, fragments: fragments, key: key})
	}
	switch class.Scope {
	case scheduling.DeviceTopologyDomainScopeNode:
		for _, key := range snapshot.LocalDomainsByClass[class] {
			if key.OwnerNodeUID != node.Identity.UID {
				continue
			}
			domain, found := snapshot.LocalDomains[key]
			if key.ResourceName != class.ResourceName || !found || domain.Key != key || domain.Class != class || domain.NodeName != nodeName ||
				domain.SourceGeneration != node.SourceGeneration {
				dataIssue = true
				continue
			}
			local := key
			appendOption(hardInstance{local: &local}, domain.EffectiveDeviceKeys, 1, 1, localDomainKeyString(key))
			if s.exhausted {
				return nil, dataIssue
			}
		}
	case scheduling.DeviceTopologyDomainScopeFabric:
		for _, key := range snapshot.FabricsByClass[class] {
			fabric, found := snapshot.Fabrics[key]
			if key.ResourceName != class.ResourceName || !found || fabric.Key != key || fabric.Class != class || !s.fabricReady(fabric) {
				dataIssue = true
				continue
			}
			memberKeys := make([]api.DeviceKey, 0)
			fragments := 0
			for _, member := range fabric.Members {
				if member.NodeName != nodeName || member.NodeUID != node.Identity.UID {
					continue
				}
				for _, domainKey := range member.LocalDomainKeys {
					domain, found := snapshot.LocalDomains[domainKey]
					if !found || domain.Key != domainKey || domain.NodeName != nodeName || domainKey.OwnerNodeUID != node.Identity.UID ||
						domainKey.ResourceName != class.ResourceName || domain.SourceGeneration != node.SourceGeneration {
						dataIssue = true
						continue
					}
					memberKeys = append(memberKeys, domain.EffectiveDeviceKeys...)
					fragments++
				}
			}
			fabricKey := key
			appendOption(hardInstance{fabric: &fabricKey}, memberKeys, len(fabric.Members), fragments, fabricKeyString(key))
			if s.exhausted {
				return nil, dataIssue
			}
		}
	}
	sort.Slice(options, func(i, j int) bool {
		left, right := options[i], options[j]
		leftExact, rightExact := int64(left.capacity) == request, int64(right.capacity) == request
		if leftExact != rightExact {
			return leftExact
		}
		if left.capacity != right.capacity {
			return left.capacity < right.capacity
		}
		if left.fanout != right.fanout {
			return left.fanout < right.fanout
		}
		if left.fragments != right.fragments {
			return left.fragments < right.fragments
		}
		return left.key < right.key
	})
	return options, dataIssue
}

func (s *hardSearch) fabricReady(fabric api.FabricDomain) bool {
	ownerFound := false
	for _, member := range fabric.Members {
		if s.nodeReady(member.NodeName) != nil {
			return false
		}
		state := s.input.Snapshot.Nodes[member.NodeName]
		if member.NodeUID != state.Identity.UID || member.SourceGeneration != state.SourceGeneration {
			return false
		}
		if member.NodeUID == fabric.OwnerNodeUID && member.SourceGeneration == fabric.SourceGeneration {
			ownerFound = true
		}
	}
	return ownerFound
}

func (s *hardSearch) usableKeys(nodeName string, class api.DomainClassKey, instance hardInstance, keys []api.DeviceKey) []api.DeviceKey {
	state := s.input.Snapshot.Nodes[nodeName]
	seen := make(map[api.DeviceKey]struct{}, len(keys))
	result := make([]api.DeviceKey, 0, len(keys))
	for _, key := range keys {
		if key.OwnerNodeUID != state.Identity.UID || key.ResourceName != class.ResourceName {
			continue
		}
		if instance.local != nil && (key.ID.ProviderID != instance.local.ID.ProviderID || key.ID.Namespace != instance.local.ID.Namespace) {
			continue
		}
		if instance.fabric != nil && (key.ID.ProviderID != instance.fabric.ProviderID || key.ID.Namespace != instance.fabric.Namespace) {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if _, occupied := s.used[key]; occupied {
			continue
		}
		device, found := s.input.Snapshot.Devices[key]
		if !found || device.Key != key || device.NodeName != nodeName || device.Health != api.DeviceHealthy ||
			device.SourceGeneration != state.SourceGeneration {
			continue
		}
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool { return lessDeviceKey(result[i], result[j]) })
	return result
}

func lessDeviceKey(left, right api.DeviceKey) bool {
	if left.ResourceName != right.ResourceName {
		return left.ResourceName < right.ResourceName
	}
	if left.OwnerNodeUID != right.OwnerNodeUID {
		return left.OwnerNodeUID < right.OwnerNodeUID
	}
	if left.ID.ProviderID != right.ID.ProviderID {
		return left.ID.ProviderID < right.ID.ProviderID
	}
	if left.ID.Namespace != right.ID.Namespace {
		return left.ID.Namespace < right.ID.Namespace
	}
	return left.ID.Value < right.ID.Value
}

func intersectKeys(left, right []api.DeviceKey) []api.DeviceKey {
	result := make([]api.DeviceKey, 0)
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i] == right[j]:
			result = append(result, left[i])
			i++
			j++
		case lessDeviceKey(left[i], right[j]):
			i++
		default:
			j++
		}
	}
	return result
}

func optionCount(step hardStep) int {
	count := 0
	for _, options := range step.options {
		count += len(options)
	}
	return count
}

func validInstance(scope scheduling.DeviceTopologyDomainScope, instance hardInstance) bool {
	switch scope {
	case scheduling.DeviceTopologyDomainScopeNode:
		return instance.local != nil && instance.fabric == nil
	case scheduling.DeviceTopologyDomainScopeFabric:
		return instance.fabric != nil && instance.local == nil
	default:
		return false
	}
}

func sameInstance(left, right hardInstance) bool {
	if left.local != nil || right.local != nil {
		return left.local != nil && right.local != nil && *left.local == *right.local
	}
	return left.fabric != nil && right.fabric != nil && *left.fabric == *right.fabric
}

func copyLocal(key *api.LocalDomainKey) *api.LocalDomainKey {
	if key == nil {
		return nil
	}
	copied := *key
	return &copied
}

func copyFabric(key *api.FabricKey) *api.FabricKey {
	if key == nil {
		return nil
	}
	copied := *key
	return &copied
}

func hardFailure(reason, format string, args ...interface{}) *HardPlanFailure {
	return &HardPlanFailure{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

func hardPolicySortKey(policy api.CanonicalDeviceTopologyPolicy) string {
	// Canonical policy contains only stable fields and sorted selector slices.
	// Include the selector so otherwise identical Pod policies have a total order.
	encoded, _ := json.Marshal(policy)
	return string(encoded)
}
