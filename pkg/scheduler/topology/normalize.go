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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

// FactValidationReason is a stable, structured classification for rejected
// provider facts. It intentionally contains no raw device or domain IDs.
type FactValidationReason string

const (
	InvalidProviderFacts     FactValidationReason = "XPUTopologyInvalidProviderFacts"
	DomainClassUnsupported   FactValidationReason = "XPUTopologyDomainClassUnsupported"
	NodeObservationOutOfDate FactValidationReason = "XPUTopologyNodeObservationOutOfDate"
)

// FactValidationError reports a source-contract problem without converting an
// invalid update into an empty/clear topology observation.
type FactValidationError struct {
	Reason  FactValidationReason
	Problem string
}

func (e *FactValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Problem)
}

func invalidFacts(format string, args ...any) error {
	return &FactValidationError{Reason: InvalidProviderFacts, Problem: fmt.Sprintf(format, args...)}
}

func unsupportedClass(format string, args ...any) error {
	return &FactValidationError{Reason: DomainClassUnsupported, Problem: fmt.Sprintf(format, args...)}
}

// PendingFabric records why a syntactically valid complete owner declaration
// is not yet publishable. It is not a fallback Fabric and cannot participate
// in topology scheduling until all members resolve at their declared versions.
type PendingFabric struct {
	Key          api.FabricKey
	OwnerNodeUID types.UID
	Reason       string
}

// CanonicalTopology is a newly constructed, caller-owned result of pure
// normalization. It shares no slices or maps with provider input or internal
// tracker state. SchedulerCache will publish a paired immutable pointer in
// XPU-06; this PR deliberately does not mutate any published cache snapshot.
type CanonicalTopology struct {
	Devices             map[api.DeviceKey]api.TopologyDevice
	LocalDomains        map[api.LocalDomainKey]api.DeviceDomain
	Fabrics             map[api.FabricKey]api.FabricDomain
	LocalDomainsByClass map[api.DomainClassKey][]api.LocalDomainKey
	FabricsByClass      map[api.DomainClassKey][]api.FabricKey
	PendingFabrics      []PendingFabric
}

// Normalizer validates one Provider contract and turns its source facts into
// NodeUID-safe canonical objects. It is pure and has no cache, goroutine,
// external I/O, scheduler scoring, or allocation responsibility.
type Normalizer struct {
	catalog      api.CatalogView
	capabilities provider.TopologyProviderCapabilities
	classes      map[api.DomainClassKey]struct{}
}

// NewNormalizer freezes the provider capability view for one process-scoped
// provider. A changed capability identity requires a process restart, so it
// is not accepted as a per-update parameter.
func NewNormalizer(catalog api.CatalogView, capabilities provider.TopologyProviderCapabilities) (*Normalizer, error) {
	if err := capabilities.Validate(catalog); err != nil {
		return nil, err
	}
	classes := make(map[api.DomainClassKey]struct{}, len(capabilities.DomainClasses))
	for _, class := range capabilities.DomainClasses {
		classes[class] = struct{}{}
	}
	capabilities.DomainClasses = append([]api.DomainClassKey(nil), capabilities.DomainClasses...)
	return &Normalizer{catalog: catalog, capabilities: capabilities, classes: classes}, nil
}

// FingerprintUpdate validates all node-local source content and returns its
// canonical fingerprint. The fingerprint deliberately excludes Node
// resourceVersion and observation timestamps so content-identical heartbeats
// may refresh freshness at the same source generation.
func (n *Normalizer) FingerprintUpdate(update provider.ProviderNodeUpdate) (string, error) {
	node, err := n.normalizeUpdate(update)
	if err != nil {
		return "", err
	}
	if update.Operation == provider.ClearFacts {
		return canonicalFingerprint(struct {
			Operation provider.ProviderUpdateOperation `json:"operation"`
		}{Operation: provider.ClearFacts})
	}
	return canonicalFingerprint(struct {
		Devices      []api.TopologyDevice      `json:"devices"`
		LocalDomains []api.DeviceDomain        `json:"localDomains"`
		Fabrics      []fabricFingerprintSource `json:"fabrics"`
	}{
		Devices:      node.devices,
		LocalDomains: node.domains,
		Fabrics:      fingerprintFabrics(node.fabrics),
	})
}

// Normalize converts the complete Provider tracker view into canonical facts.
// ClearFacts remains a Synced state but contributes no inventory. A fabric
// whose member is Pending, cleared, replaced at another generation, or missing
// a referenced local domain is reported in PendingFabrics rather than guessed.
func (n *Normalizer) Normalize(records []provider.ProviderNodeRecord) (CanonicalTopology, error) {
	result := CanonicalTopology{
		Devices:             make(map[api.DeviceKey]api.TopologyDevice),
		LocalDomains:        make(map[api.LocalDomainKey]api.DeviceDomain),
		Fabrics:             make(map[api.FabricKey]api.FabricDomain),
		LocalDomainsByClass: make(map[api.DomainClassKey][]api.LocalDomainKey),
		FabricsByClass:      make(map[api.DomainClassKey][]api.FabricKey),
	}

	sortedRecords := append([]provider.ProviderNodeRecord(nil), records...)
	sort.Slice(sortedRecords, func(i, j int) bool {
		return providerKeyString(sortedRecords[i].Key) < providerKeyString(sortedRecords[j].Key)
	})

	nodes := make(map[string]normalizedNode)
	fabricSources := make(map[api.FabricKey]normalizedFabricSource)
	for _, record := range sortedRecords {
		if record.State != provider.ProviderNodeSynced {
			continue
		}
		if record.Key != record.Update.Key() {
			return CanonicalTopology{}, invalidFacts("record key does not match update identity")
		}
		node, err := n.normalizeUpdate(record.Update)
		if err != nil {
			return CanonicalTopology{}, err
		}
		if record.Update.Operation == provider.ClearFacts {
			continue
		}
		if existing, found := nodes[node.identity.Name]; found && existing.identity.UID != node.identity.UID {
			return CanonicalTopology{}, invalidFacts("NodeName %q has more than one active NodeUID", node.identity.Name)
		}
		nodes[node.identity.Name] = node
		for _, device := range node.devices {
			if _, exists := result.Devices[device.Key]; exists {
				return CanonicalTopology{}, invalidFacts("duplicate canonical DeviceKey for NodeUID %q", device.Key.OwnerNodeUID)
			}
			result.Devices[device.Key] = cloneTopologyDevice(device)
		}
		for _, domain := range node.domains {
			if _, exists := result.LocalDomains[domain.Key]; exists {
				return CanonicalTopology{}, invalidFacts("duplicate canonical LocalDomainKey for NodeUID %q", domain.Key.OwnerNodeUID)
			}
			result.LocalDomains[domain.Key] = cloneDeviceDomain(domain)
			result.LocalDomainsByClass[domain.Class] = append(result.LocalDomainsByClass[domain.Class], domain.Key)
		}
		for _, fabric := range node.fabrics {
			if existing, found := fabricSources[fabric.key]; found {
				return CanonicalTopology{}, invalidFacts("FabricKey %q has multiple owners %q and %q", fabric.key.Value, existing.ownerNodeName, fabric.ownerNodeName)
			}
			fabricSources[fabric.key] = fabric
		}
	}

	for _, keys := range result.LocalDomainsByClass {
		sortLocalDomainKeys(keys)
	}

	fabricKeys := make([]api.FabricKey, 0, len(fabricSources))
	for key := range fabricSources {
		fabricKeys = append(fabricKeys, key)
	}
	sortFabricKeys(fabricKeys)
	for _, key := range fabricKeys {
		fabric := fabricSources[key]
		resolved, pending := resolveFabric(fabric, nodes)
		if pending != "" {
			result.PendingFabrics = append(result.PendingFabrics, PendingFabric{Key: key, OwnerNodeUID: fabric.ownerNodeUID, Reason: pending})
			continue
		}
		result.Fabrics[key] = resolved
		result.FabricsByClass[resolved.Class] = append(result.FabricsByClass[resolved.Class], key)
	}
	for _, keys := range result.FabricsByClass {
		sortFabricKeys(keys)
	}
	sort.Slice(result.PendingFabrics, func(i, j int) bool {
		return fabricKeyString(result.PendingFabrics[i].Key) < fabricKeyString(result.PendingFabrics[j].Key)
	})
	return result, nil
}

type normalizedNode struct {
	identity api.NodeIdentity
	devices  []api.TopologyDevice
	domains  []api.DeviceDomain
	fabrics  []normalizedFabricSource
}

type normalizedFabricSource struct {
	key              api.FabricKey
	class            api.DomainClassKey
	ownerNodeName    string
	ownerNodeUID     types.UID
	sourceGeneration uint64
	members          []normalizedFabricMemberSource
}

type normalizedFabricMemberSource struct {
	nodeName         string
	sourceGeneration uint64
	localDomainIDs   []api.SourceDomainID
}

// fabricFingerprintSource is deliberately exported-to-JSON rather than
// serializing normalizedFabricSource directly: its implementation fields are
// private, and an empty JSON object would incorrectly let same-generation
// Fabric membership changes pass as content-identical heartbeats.
type fabricFingerprintSource struct {
	Key              api.FabricKey                   `json:"key"`
	Class            api.DomainClassKey              `json:"class"`
	OwnerNodeName    string                          `json:"ownerNodeName"`
	OwnerNodeUID     types.UID                       `json:"ownerNodeUID"`
	SourceGeneration uint64                          `json:"sourceGeneration"`
	Members          []fabricFingerprintMemberSource `json:"members"`
}

type fabricFingerprintMemberSource struct {
	NodeName         string               `json:"nodeName"`
	SourceGeneration uint64               `json:"sourceGeneration"`
	LocalDomainIDs   []api.SourceDomainID `json:"localDomainIDs"`
}

func fingerprintFabrics(fabrics []normalizedFabricSource) []fabricFingerprintSource {
	result := make([]fabricFingerprintSource, len(fabrics))
	for index, fabric := range fabrics {
		members := make([]fabricFingerprintMemberSource, len(fabric.members))
		for memberIndex, member := range fabric.members {
			members[memberIndex] = fabricFingerprintMemberSource{
				NodeName:         member.nodeName,
				SourceGeneration: member.sourceGeneration,
				LocalDomainIDs:   append([]api.SourceDomainID(nil), member.localDomainIDs...),
			}
		}
		result[index] = fabricFingerprintSource{
			Key: fabric.key, Class: fabric.class, OwnerNodeName: fabric.ownerNodeName,
			OwnerNodeUID: fabric.ownerNodeUID, SourceGeneration: fabric.sourceGeneration, Members: members,
		}
	}
	return result
}

func (n *Normalizer) normalizeUpdate(update provider.ProviderNodeUpdate) (normalizedNode, error) {
	if err := update.ValidateShape(); err != nil {
		return normalizedNode{}, invalidFacts("%v", err)
	}
	if update.ProviderID != n.capabilities.Identity.ProviderID || update.IdentityNamespace != n.capabilities.Identity.Namespace || update.ResourceName != n.capabilities.ResourceName {
		return normalizedNode{}, invalidFacts("update ProviderID/namespace/resource does not match fixed provider capability")
	}
	node := normalizedNode{identity: api.NodeIdentity{Name: update.NodeName, UID: update.NodeUID, ResourceVersion: update.NodeResourceVersion}}
	if update.Operation == provider.ClearFacts {
		return node, nil
	}
	facts := update.Facts
	if facts.ResourceName != update.ResourceName {
		return normalizedNode{}, invalidFacts("facts resource %q does not match update resource %q", facts.ResourceName, update.ResourceName)
	}
	if len(facts.Devices) == 0 {
		return normalizedNode{}, invalidFacts("empty inventory must use ClearFacts")
	}

	devicesByID := make(map[api.SourceDeviceID]api.TopologyDevice, len(facts.Devices))
	for _, fact := range facts.Devices {
		if strings.TrimSpace(string(fact.ID)) == "" {
			return normalizedNode{}, invalidFacts("device ID is required")
		}
		if _, exists := devicesByID[fact.ID]; exists {
			return normalizedNode{}, invalidFacts("duplicate source device ID %q on NodeUID %q", fact.ID, update.NodeUID)
		}
		if fact.Health != api.DeviceHealthy && fact.Health != api.DeviceUnhealthy && fact.Health != api.DeviceHealthUnknown {
			return normalizedNode{}, invalidFacts("device %q has unsupported health %q", fact.ID, fact.Health)
		}
		devicesByID[fact.ID] = api.TopologyDevice{
			Key: api.DeviceKey{
				ResourceName: update.ResourceName,
				OwnerNodeUID: update.NodeUID,
				ID:           api.DeviceID{ProviderID: update.ProviderID, Namespace: update.IdentityNamespace, Value: fact.ID},
			},
			NodeName:         update.NodeName,
			Health:           fact.Health,
			SourceGeneration: update.SourceGeneration,
		}
	}

	domainsByID := make(map[api.SourceDomainID]api.DeviceDomain, len(facts.LocalDomains))
	directDomainIDs := make(map[api.SourceDomainID][]api.SourceDomainID, len(facts.LocalDomains))
	for _, fact := range facts.LocalDomains {
		if strings.TrimSpace(string(fact.ID)) == "" {
			return normalizedNode{}, invalidFacts("local domain ID is required")
		}
		if _, exists := domainsByID[fact.ID]; exists {
			return normalizedNode{}, invalidFacts("duplicate source local domain ID %q on NodeUID %q", fact.ID, update.NodeUID)
		}
		class, err := n.resolveClass(update.ResourceName, scheduling.DeviceTopologyDomainScopeNode, fact.DomainClass)
		if err != nil {
			return normalizedNode{}, err
		}
		memberDevices, err := deviceKeysForFact(fact.DeviceIDs, devicesByID)
		if err != nil {
			return normalizedNode{}, err
		}
		if len(memberDevices) == 0 && len(fact.DomainIDs) == 0 {
			return normalizedNode{}, invalidFacts("local domain %q must explicitly contain a device or local domain", fact.ID)
		}
		if hasDuplicateSourceDomainIDs(fact.DomainIDs) {
			return normalizedNode{}, invalidFacts("local domain %q has duplicate member domain IDs", fact.ID)
		}
		domainsByID[fact.ID] = api.DeviceDomain{
			Key: api.LocalDomainKey{
				ResourceName: update.ResourceName,
				OwnerNodeUID: update.NodeUID,
				ID:           api.DomainID{ProviderID: update.ProviderID, Namespace: update.IdentityNamespace, Value: fact.ID},
			},
			Class:            class,
			NodeName:         update.NodeName,
			MemberDeviceKeys: memberDevices,
			SourceGeneration: update.SourceGeneration,
		}
		directDomainIDs[fact.ID] = append([]api.SourceDomainID(nil), fact.DomainIDs...)
	}

	// Resolve direct child domains and then compute the acyclic, de-duplicated
	// transitive device closure for every local domain.
	for id, childIDs := range directDomainIDs {
		domain := domainsByID[id]
		children := make([]api.LocalDomainKey, 0, len(childIDs))
		for _, childID := range childIDs {
			child, found := domainsByID[childID]
			if !found {
				return normalizedNode{}, invalidFacts("local domain %q references missing local domain %q", id, childID)
			}
			children = append(children, child.Key)
		}
		sortLocalDomainKeys(children)
		domain.MemberDomainKeys = children
		domainsByID[id] = domain
	}

	states := make(map[api.SourceDomainID]visitState, len(domainsByID))
	var resolveEffective func(api.SourceDomainID) ([]api.DeviceKey, error)
	resolveEffective = func(id api.SourceDomainID) ([]api.DeviceKey, error) {
		switch states[id] {
		case visitActive:
			return nil, invalidFacts("local domain membership contains a cycle at %q", id)
		case visitDone:
			return append([]api.DeviceKey(nil), domainsByID[id].EffectiveDeviceKeys...), nil
		}
		states[id] = visitActive
		domain := domainsByID[id]
		effective := append([]api.DeviceKey(nil), domain.MemberDeviceKeys...)
		for _, childKey := range domain.MemberDomainKeys {
			child, found := domainsByID[childKey.ID.Value]
			if !found {
				return nil, invalidFacts("local domain %q references unknown canonical child", id)
			}
			childEffective, err := resolveEffective(child.Key.ID.Value)
			if err != nil {
				return nil, err
			}
			effective = append(effective, childEffective...)
		}
		domain.EffectiveDeviceKeys = compactDeviceKeys(effective)
		fingerprint, err := canonicalFingerprint(struct {
			Class        api.DomainClassKey   `json:"class"`
			Devices      []api.DeviceKey      `json:"devices"`
			ChildDomains []api.LocalDomainKey `json:"childDomains"`
			Effective    []api.DeviceKey      `json:"effective"`
		}{domain.Class, domain.MemberDeviceKeys, domain.MemberDomainKeys, domain.EffectiveDeviceKeys})
		if err != nil {
			return nil, err
		}
		domain.MembershipFingerprint = fingerprint
		domainsByID[id] = domain
		states[id] = visitDone
		return append([]api.DeviceKey(nil), domain.EffectiveDeviceKeys...), nil
	}

	deviceDomains := make(map[api.SourceDeviceID][]api.LocalDomainKey, len(devicesByID))
	domainIDs := make([]api.SourceDomainID, 0, len(domainsByID))
	for id := range domainsByID {
		domainIDs = append(domainIDs, id)
	}
	sort.Slice(domainIDs, func(i, j int) bool { return domainIDs[i] < domainIDs[j] })
	for _, id := range domainIDs {
		if _, err := resolveEffective(id); err != nil {
			return normalizedNode{}, err
		}
		domain := domainsByID[id]
		for _, key := range domain.MemberDeviceKeys {
			deviceDomains[key.ID.Value] = append(deviceDomains[key.ID.Value], domain.Key)
		}
	}

	deviceIDs := make([]api.SourceDeviceID, 0, len(devicesByID))
	for id := range devicesByID {
		deviceIDs = append(deviceIDs, id)
	}
	sort.Slice(deviceIDs, func(i, j int) bool { return deviceIDs[i] < deviceIDs[j] })
	node.devices = make([]api.TopologyDevice, 0, len(deviceIDs))
	for _, id := range deviceIDs {
		device := devicesByID[id]
		device.LocalDomainKeys = append([]api.LocalDomainKey(nil), deviceDomains[id]...)
		sortLocalDomainKeys(device.LocalDomainKeys)
		fingerprint, err := canonicalFingerprint(struct {
			LocalDomains []api.LocalDomainKey `json:"localDomains"`
		}{device.LocalDomainKeys})
		if err != nil {
			return normalizedNode{}, err
		}
		device.MembershipFingerprint = fingerprint
		node.devices = append(node.devices, device)
	}
	node.domains = make([]api.DeviceDomain, 0, len(domainIDs))
	for _, id := range domainIDs {
		node.domains = append(node.domains, domainsByID[id])
	}

	fabricIDs := make(map[api.SourceFabricID]struct{}, len(facts.Fabrics))
	node.fabrics = make([]normalizedFabricSource, 0, len(facts.Fabrics))
	for _, fact := range facts.Fabrics {
		if strings.TrimSpace(string(fact.ID)) == "" {
			return normalizedNode{}, invalidFacts("fabric ID is required")
		}
		if _, exists := fabricIDs[fact.ID]; exists {
			return normalizedNode{}, invalidFacts("duplicate source Fabric ID %q on NodeUID %q", fact.ID, update.NodeUID)
		}
		fabricIDs[fact.ID] = struct{}{}
		if fact.OwnerNode != update.NodeName {
			return normalizedNode{}, invalidFacts("fabric %q ownerNode %q must equal publishing NodeName %q", fact.ID, fact.OwnerNode, update.NodeName)
		}
		class, err := n.resolveClass(update.ResourceName, scheduling.DeviceTopologyDomainScopeFabric, fact.DomainClass)
		if err != nil {
			return normalizedNode{}, err
		}
		members, err := normalizeFabricMembers(fact, update)
		if err != nil {
			return normalizedNode{}, err
		}
		node.fabrics = append(node.fabrics, normalizedFabricSource{
			key:   api.FabricKey{ProviderID: update.ProviderID, Namespace: update.IdentityNamespace, ResourceName: update.ResourceName, Value: fact.ID},
			class: class, ownerNodeName: update.NodeName, ownerNodeUID: update.NodeUID,
			sourceGeneration: update.SourceGeneration, members: members,
		})
	}
	sort.Slice(node.fabrics, func(i, j int) bool {
		return fabricKeyString(node.fabrics[i].key) < fabricKeyString(node.fabrics[j].key)
	})
	return node, nil
}

type visitState uint8

const (
	visitNew visitState = iota
	visitActive
	visitDone
)

func (n *Normalizer) resolveClass(resourceName corev1.ResourceName, scope scheduling.DeviceTopologyDomainScope, name scheduling.DeviceTopologyDomainClass) (api.DomainClassKey, error) {
	key := api.DomainClassKey{ResourceName: resourceName, Scope: scope, Name: name}
	if !n.catalog.HasDomainClass(key) {
		return api.DomainClassKey{}, unsupportedClass("domain class %q/%q is not present in the fixed catalog for resource %q", scope, name, resourceName)
	}
	if _, found := n.classes[key]; !found {
		return api.DomainClassKey{}, unsupportedClass("domain class %q/%q is not declared by provider %q", scope, name, n.capabilities.Identity.ProviderID)
	}
	return key, nil
}

func deviceKeysForFact(ids []api.SourceDeviceID, devices map[api.SourceDeviceID]api.TopologyDevice) ([]api.DeviceKey, error) {
	if hasDuplicateSourceDeviceIDs(ids) {
		return nil, invalidFacts("local domain has duplicate member device IDs")
	}
	keys := make([]api.DeviceKey, 0, len(ids))
	for _, id := range ids {
		device, found := devices[id]
		if !found {
			return nil, invalidFacts("local domain references missing device ID %q", id)
		}
		keys = append(keys, device.Key)
	}
	sortDeviceKeys(keys)
	return keys, nil
}

func normalizeFabricMembers(fabric provider.FabricFact, update provider.ProviderNodeUpdate) ([]normalizedFabricMemberSource, error) {
	if len(fabric.Members) == 0 {
		return nil, invalidFacts("fabric %q must declare a complete non-empty member set", fabric.ID)
	}
	seenNodes := make(map[string]struct{}, len(fabric.Members))
	foundOwner := false
	members := make([]normalizedFabricMemberSource, 0, len(fabric.Members))
	for _, member := range fabric.Members {
		if strings.TrimSpace(member.NodeName) == "" {
			return nil, invalidFacts("fabric %q has a member without a NodeName", fabric.ID)
		}
		if member.SourceGeneration == 0 {
			return nil, invalidFacts("fabric %q member %q must declare sourceGeneration", fabric.ID, member.NodeName)
		}
		if _, exists := seenNodes[member.NodeName]; exists {
			return nil, invalidFacts("fabric %q has duplicate member NodeName %q", fabric.ID, member.NodeName)
		}
		seenNodes[member.NodeName] = struct{}{}
		if len(member.LocalDomainIDs) == 0 || hasDuplicateSourceDomainIDs(member.LocalDomainIDs) {
			return nil, invalidFacts("fabric %q member %q must reference unique local domains", fabric.ID, member.NodeName)
		}
		if member.NodeName == update.NodeName {
			foundOwner = true
			if member.SourceGeneration != update.SourceGeneration {
				return nil, invalidFacts("fabric %q owner member generation %d must equal publishing generation %d", fabric.ID, member.SourceGeneration, update.SourceGeneration)
			}
		}
		localDomainIDs := append([]api.SourceDomainID(nil), member.LocalDomainIDs...)
		sort.Slice(localDomainIDs, func(i, j int) bool { return localDomainIDs[i] < localDomainIDs[j] })
		members = append(members, normalizedFabricMemberSource{nodeName: member.NodeName, sourceGeneration: member.SourceGeneration, localDomainIDs: localDomainIDs})
	}
	if !foundOwner {
		return nil, invalidFacts("fabric %q owner %q must occur in its complete member set", fabric.ID, update.NodeName)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].nodeName < members[j].nodeName })
	return members, nil
}

func resolveFabric(source normalizedFabricSource, nodes map[string]normalizedNode) (api.FabricDomain, string) {
	members := make([]api.FabricMember, 0, len(source.members))
	for _, sourceMember := range source.members {
		node, found := nodes[sourceMember.nodeName]
		if !found {
			return api.FabricDomain{}, fmt.Sprintf("member Node %q is not Synced", sourceMember.nodeName)
		}
		if node.devices == nil {
			return api.FabricDomain{}, fmt.Sprintf("member Node %q has no active facts", sourceMember.nodeName)
		}
		if sourceMember.sourceGeneration != sourceGeneration(node) {
			return api.FabricDomain{}, fmt.Sprintf("member Node %q generation %d does not match declared generation %d", sourceMember.nodeName, sourceGeneration(node), sourceMember.sourceGeneration)
		}
		domainKeys := make([]api.LocalDomainKey, 0, len(sourceMember.localDomainIDs))
		for _, sourceDomainID := range sourceMember.localDomainIDs {
			domain, found := node.domainBySourceID(sourceDomainID)
			if !found {
				return api.FabricDomain{}, fmt.Sprintf("member Node %q lacks local domain %q", sourceMember.nodeName, sourceDomainID)
			}
			domainKeys = append(domainKeys, domain.Key)
		}
		sortLocalDomainKeys(domainKeys)
		members = append(members, api.FabricMember{
			NodeName:         node.identity.Name,
			NodeUID:          node.identity.UID,
			SourceGeneration: sourceGeneration(node),
			LocalDomainKeys:  domainKeys,
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].NodeName < members[j].NodeName })
	fabric := api.FabricDomain{Key: source.key, Class: source.class, OwnerNodeUID: source.ownerNodeUID, SourceGeneration: source.sourceGeneration, Members: members}
	fingerprint, err := canonicalFingerprint(struct {
		Class   api.DomainClassKey `json:"class"`
		Members []api.FabricMember `json:"members"`
	}{fabric.Class, fabric.Members})
	if err != nil {
		return api.FabricDomain{}, err.Error()
	}
	fabric.MembershipFingerprint = fingerprint
	return fabric, ""
}

func sourceGeneration(node normalizedNode) uint64 {
	if len(node.devices) == 0 {
		return 0
	}
	return node.devices[0].SourceGeneration
}

func (n normalizedNode) domainBySourceID(id api.SourceDomainID) (api.DeviceDomain, bool) {
	for _, domain := range n.domains {
		if domain.Key.ID.Value == id {
			return domain, true
		}
	}
	return api.DeviceDomain{}, false
}

func hasDuplicateSourceDeviceIDs(ids []api.SourceDeviceID) bool {
	seen := make(map[api.SourceDeviceID]struct{}, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

func hasDuplicateSourceDomainIDs(ids []api.SourceDomainID) bool {
	seen := make(map[api.SourceDomainID]struct{}, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; exists {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

func compactDeviceKeys(keys []api.DeviceKey) []api.DeviceKey {
	sortDeviceKeys(keys)
	if len(keys) < 2 {
		return keys
	}
	write := 1
	for read := 1; read < len(keys); read++ {
		if keys[read] == keys[write-1] {
			continue
		}
		keys[write] = keys[read]
		write++
	}
	return keys[:write]
}

func sortDeviceKeys(keys []api.DeviceKey) {
	sort.Slice(keys, func(i, j int) bool { return deviceKeyString(keys[i]) < deviceKeyString(keys[j]) })
}

func sortLocalDomainKeys(keys []api.LocalDomainKey) {
	sort.Slice(keys, func(i, j int) bool { return localDomainKeyString(keys[i]) < localDomainKeyString(keys[j]) })
}

func sortFabricKeys(keys []api.FabricKey) {
	sort.Slice(keys, func(i, j int) bool { return fabricKeyString(keys[i]) < fabricKeyString(keys[j]) })
}

func deviceKeyString(key api.DeviceKey) string {
	return strings.Join([]string{string(key.ResourceName), string(key.OwnerNodeUID), key.ID.ProviderID, key.ID.Namespace, string(key.ID.Value)}, "\x00")
}

func localDomainKeyString(key api.LocalDomainKey) string {
	return strings.Join([]string{string(key.ResourceName), string(key.OwnerNodeUID), key.ID.ProviderID, key.ID.Namespace, string(key.ID.Value)}, "\x00")
}

func fabricKeyString(key api.FabricKey) string {
	return strings.Join([]string{key.ProviderID, key.Namespace, string(key.ResourceName), string(key.Value)}, "\x00")
}

func providerKeyString(key provider.ProviderNodeKey) string {
	return strings.Join([]string{key.ProviderID, key.Namespace, string(key.ResourceName), string(key.NodeUID)}, "\x00")
}

func canonicalFingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal canonical topology: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneTopologyDevice(device api.TopologyDevice) api.TopologyDevice {
	device.LocalDomainKeys = append([]api.LocalDomainKey(nil), device.LocalDomainKeys...)
	return device
}

func cloneDeviceDomain(domain api.DeviceDomain) api.DeviceDomain {
	domain.MemberDeviceKeys = append([]api.DeviceKey(nil), domain.MemberDeviceKeys...)
	domain.MemberDomainKeys = append([]api.LocalDomainKey(nil), domain.MemberDomainKeys...)
	domain.EffectiveDeviceKeys = append([]api.DeviceKey(nil), domain.EffectiveDeviceKeys...)
	return domain
}
