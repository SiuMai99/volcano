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

package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// SourceDeviceID is an opaque device identity supplied by one validated
// Provider contract. It is never a scheduler key on its own.
type SourceDeviceID string

// SourceDomainID is an opaque node-local domain identity supplied by one
// validated Provider contract.
type SourceDomainID string

// SourceFabricID is an opaque fabric identity supplied by one validated
// Provider contract.
type SourceFabricID string

// DeviceID is the Provider-normalized identity of one device. Node ownership
// and the resource are deliberately outside this type.
type DeviceID struct {
	ProviderID string
	Namespace  string
	Value      SourceDeviceID
}

// DeviceKey is the scheduler identity of one Node-owned allocatable device.
// A NodeName, GPU index, or source ID alone is never sufficient identity.
type DeviceKey struct {
	ResourceName corev1.ResourceName
	OwnerNodeUID types.UID
	ID           DeviceID
}

// DomainID is the Provider-normalized identity of one node-local domain. It
// becomes unique only when combined with a resource and owner NodeUID.
type DomainID struct {
	ProviderID string
	Namespace  string
	Value      SourceDomainID
}

// LocalDomainKey is the canonical identity of one Node-local domain.
type LocalDomainKey struct {
	ResourceName corev1.ResourceName
	OwnerNodeUID types.UID
	ID           DomainID
}

// FabricID is cluster-scoped only within one Provider identity contract and
// resource. Concrete membership retains NodeUIDs on FabricDomain.
type FabricID struct {
	ProviderID   string
	Namespace    string
	ResourceName corev1.ResourceName
	Value        SourceFabricID
}

// FabricKey is the canonical identity of a Provider-declared fabric.
type FabricKey = FabricID

// DomainClassKey identifies an administrator-defined topology class. It is a
// class contract, not the identity of a concrete Domain or Fabric instance.
type DomainClassKey struct {
	ResourceName corev1.ResourceName
	Scope        scheduling.DeviceTopologyDomainScope
	Name         scheduling.DeviceTopologyDomainClass
}

// DomainClassDescriptor is one fixed catalog class definition.
type DomainClassDescriptor struct {
	Key         DomainClassKey
	Description string
}

// ResourceTopologyDescriptor is the scheduler's immutable in-memory view of
// one resource's catalog definition. It intentionally has no revision or
// catalog-version field: Alpha catalogs are fixed for a process lifetime.
type ResourceTopologyDescriptor struct {
	ResourceName corev1.ResourceName
	Classes      []DomainClassDescriptor
	Description  string
}

// DeviceHealth is the Provider-observed health of a topology device.
type DeviceHealth string

const (
	// DeviceHealthy is usable Provider evidence.
	DeviceHealthy DeviceHealth = "Healthy"
	// DeviceUnhealthy is unusable Provider evidence.
	DeviceUnhealthy DeviceHealth = "Unhealthy"
	// DeviceHealthUnknown is insufficient Provider evidence.
	DeviceHealthUnknown DeviceHealth = "Unknown"
)

// NodeIdentity identifies one Kubernetes Node incarnation. NodeUID is the
// immutable incarnation identity; Name is retained for lookup and diagnostics.
type NodeIdentity struct {
	Name string
	UID  types.UID
}

// NodeObservation carries mutable metadata observed alongside a NodeIdentity.
// ResourceVersion is source-correlation metadata only; it is not part of the
// Node incarnation identity and must not invalidate topology facts on its own.
type NodeObservation struct {
	Identity        NodeIdentity
	ResourceVersion string
}

// DeviceTopologyNodeSyncState describes the topology source state paired with
// one concrete Kubernetes Node observation. Pending is deliberately distinct
// from an empty inventory: a new Node incarnation must not inherit facts from
// the same NodeName's previous UID.
type DeviceTopologyNodeSyncState string

const (
	// DeviceTopologyNodePending has no valid topology observation for this
	// exact Node identity yet.
	DeviceTopologyNodePending DeviceTopologyNodeSyncState = "Pending"
	// DeviceTopologyNodeSynced has a valid Provider observation at the recorded
	// source generation.
	DeviceTopologyNodeSynced DeviceTopologyNodeSyncState = "Synced"
)

// DeviceTopologyNodeState is the immutable-by-copy per-Node readiness view
// carried by a DeviceTopologySnapshot.
type DeviceTopologyNodeState struct {
	Identity         NodeIdentity
	SyncState        DeviceTopologyNodeSyncState
	SourceGeneration uint64
	// FreshUntil is copied from the accepted Provider observation. A zero value
	// means the Provider did not attach an expiry. Consumers must treat an
	// expired non-zero value as unavailable evidence; it must never cause a
	// fallback to a different Node incarnation or an inferred topology.
	FreshUntil time.Time
}

// PendingFabric is a normalized Fabric declaration that cannot yet be used
// because one or more explicitly declared members are unavailable. It is not
// a fallback Fabric and has no scheduling side effect by itself.
type PendingFabric struct {
	Key          FabricKey
	OwnerNodeUID types.UID
	Reason       string
}

// TopologyDevice is the canonical Provider fact for a concrete device.
type TopologyDevice struct {
	Key                   DeviceKey
	NodeName              string
	LocalDomainKeys       []LocalDomainKey
	Health                DeviceHealth
	SourceGeneration      uint64
	MembershipFingerprint string
}

// DeviceDomain is a class-bearing node-local topology domain. Membership is
// explicit; later normalization computes EffectiveDeviceKeys without deriving
// membership from Node or HyperNode relationships.
type DeviceDomain struct {
	Key                   LocalDomainKey
	Class                 DomainClassKey
	NodeName              string
	MemberDeviceKeys      []DeviceKey
	MemberDomainKeys      []LocalDomainKey
	EffectiveDeviceKeys   []DeviceKey
	SourceGeneration      uint64
	MembershipFingerprint string
}

// FabricMember is an explicitly resolved member of a Provider-declared
// fabric. NodeName is diagnostic only; membership identity is NodeUID plus
// source generation and local domains.
type FabricMember struct {
	NodeName         string
	NodeUID          types.UID
	SourceGeneration uint64
	LocalDomainKeys  []LocalDomainKey
}

// FabricDomain is a class-bearing Provider-declared fabric. Its owner and
// members are explicit so a HyperNode or network reachability cannot create a
// fabric implicitly.
type FabricDomain struct {
	Key                   FabricKey
	Class                 DomainClassKey
	OwnerNodeUID          types.UID
	SourceGeneration      uint64
	Members               []FabricMember
	MembershipFingerprint string
}

// DeviceTopologySnapshot is the cache-published, immutable-by-copy topology
// view paired with ClusterInfo. Callers must treat all maps, slices, and
// nested objects as read-only. Constructors below copy every mutable input so
// Provider-owned source objects and later publications cannot alter a Session
// that already holds this pointer.
//
// The snapshot intentionally contains topology facts, readiness, and the
// Provider's fixed class capability only. It does not track allocations,
// subtract normal Node resources, or encode a scheduler-selected DeviceKey.
type DeviceTopologySnapshot struct {
	Nodes               map[string]DeviceTopologyNodeState
	Devices             map[DeviceKey]TopologyDevice
	LocalDomains        map[LocalDomainKey]DeviceDomain
	Fabrics             map[FabricKey]FabricDomain
	LocalDomainsByClass map[DomainClassKey][]LocalDomainKey
	FabricsByClass      map[DomainClassKey][]FabricKey
	PendingFabrics      []PendingFabric
	// ProviderCapabilitiesKnown distinguishes a published fixed capability
	// declaration from an older/unavailable snapshot that cannot make an
	// unsupported-versus-no-facts decision.
	ProviderCapabilitiesKnown bool
	// SupportedProviderDomainClasses is the process-fixed Provider declaration
	// copied with this snapshot. It is never inferred from observed inventory.
	SupportedProviderDomainClasses map[DomainClassKey]struct{}
}

// NewDeviceTopologySnapshot constructs one caller-owned immutable-by-copy
// view. It is safe to publish the returned pointer after this function
// returns; no input map, slice, or nested member remains shared with it.
func NewDeviceTopologySnapshot(
	nodes map[string]DeviceTopologyNodeState,
	devices map[DeviceKey]TopologyDevice,
	localDomains map[LocalDomainKey]DeviceDomain,
	fabrics map[FabricKey]FabricDomain,
	localDomainsByClass map[DomainClassKey][]LocalDomainKey,
	fabricsByClass map[DomainClassKey][]FabricKey,
	pendingFabrics []PendingFabric,
) *DeviceTopologySnapshot {
	return newDeviceTopologySnapshot(nodes, devices, localDomains, fabrics, localDomainsByClass, fabricsByClass, pendingFabrics, false, nil)
}

// NewDeviceTopologySnapshotWithProviderCapabilities is the immutable-by-copy
// form used by SchedulerCache once the process-scoped Provider has declared
// its fixed supported classes. A known empty set remains distinct from an
// unavailable declaration so consumers can distinguish unsupported classes
// from absent or stale facts without inferring capability from inventory.
func NewDeviceTopologySnapshotWithProviderCapabilities(
	nodes map[string]DeviceTopologyNodeState,
	devices map[DeviceKey]TopologyDevice,
	localDomains map[LocalDomainKey]DeviceDomain,
	fabrics map[FabricKey]FabricDomain,
	localDomainsByClass map[DomainClassKey][]LocalDomainKey,
	fabricsByClass map[DomainClassKey][]FabricKey,
	pendingFabrics []PendingFabric,
	supportedClasses []DomainClassKey,
) *DeviceTopologySnapshot {
	supported := make(map[DomainClassKey]struct{}, len(supportedClasses))
	for _, class := range supportedClasses {
		supported[class] = struct{}{}
	}
	return newDeviceTopologySnapshot(nodes, devices, localDomains, fabrics, localDomainsByClass, fabricsByClass, pendingFabrics, true, supported)
}

func newDeviceTopologySnapshot(
	nodes map[string]DeviceTopologyNodeState,
	devices map[DeviceKey]TopologyDevice,
	localDomains map[LocalDomainKey]DeviceDomain,
	fabrics map[FabricKey]FabricDomain,
	localDomainsByClass map[DomainClassKey][]LocalDomainKey,
	fabricsByClass map[DomainClassKey][]FabricKey,
	pendingFabrics []PendingFabric,
	providerCapabilitiesKnown bool,
	supportedProviderDomainClasses map[DomainClassKey]struct{},
) *DeviceTopologySnapshot {
	result := &DeviceTopologySnapshot{
		Nodes:                          make(map[string]DeviceTopologyNodeState, len(nodes)),
		Devices:                        make(map[DeviceKey]TopologyDevice, len(devices)),
		LocalDomains:                   make(map[LocalDomainKey]DeviceDomain, len(localDomains)),
		Fabrics:                        make(map[FabricKey]FabricDomain, len(fabrics)),
		LocalDomainsByClass:            make(map[DomainClassKey][]LocalDomainKey, len(localDomainsByClass)),
		FabricsByClass:                 make(map[DomainClassKey][]FabricKey, len(fabricsByClass)),
		PendingFabrics:                 append([]PendingFabric(nil), pendingFabrics...),
		ProviderCapabilitiesKnown:      providerCapabilitiesKnown,
		SupportedProviderDomainClasses: make(map[DomainClassKey]struct{}, len(supportedProviderDomainClasses)),
	}
	for name, state := range nodes {
		result.Nodes[name] = state
	}
	for key, device := range devices {
		device.LocalDomainKeys = append([]LocalDomainKey(nil), device.LocalDomainKeys...)
		result.Devices[key] = device
	}
	for key, domain := range localDomains {
		domain.MemberDeviceKeys = append([]DeviceKey(nil), domain.MemberDeviceKeys...)
		domain.MemberDomainKeys = append([]LocalDomainKey(nil), domain.MemberDomainKeys...)
		domain.EffectiveDeviceKeys = append([]DeviceKey(nil), domain.EffectiveDeviceKeys...)
		result.LocalDomains[key] = domain
	}
	for key, fabric := range fabrics {
		fabric.Members = append([]FabricMember(nil), fabric.Members...)
		for index := range fabric.Members {
			fabric.Members[index].LocalDomainKeys = append([]LocalDomainKey(nil), fabric.Members[index].LocalDomainKeys...)
		}
		result.Fabrics[key] = fabric
	}
	for key, domainKeys := range localDomainsByClass {
		result.LocalDomainsByClass[key] = append([]LocalDomainKey(nil), domainKeys...)
	}
	for key, fabricKeys := range fabricsByClass {
		result.FabricsByClass[key] = append([]FabricKey(nil), fabricKeys...)
	}
	for class := range supportedProviderDomainClasses {
		result.SupportedProviderDomainClasses[class] = struct{}{}
	}
	return result
}

// FilterDeviceTopologySnapshot returns a new immutable-by-copy snapshot that
// retains only facts still paired with a Synced Node identity and source
// generation. It is used for lightweight Node invalidation: a same-name Node
// replacement therefore cannot observe the previous incarnation's facts.
func FilterDeviceTopologySnapshot(source *DeviceTopologySnapshot, nodes map[string]DeviceTopologyNodeState) *DeviceTopologySnapshot {
	if source == nil {
		return NewDeviceTopologySnapshot(nodes, nil, nil, nil, nil, nil, nil)
	}

	nodeFor := func(name string, uid types.UID, generation uint64) bool {
		state, found := nodes[name]
		return found && state.Identity.Name == name && state.Identity.UID == uid &&
			state.SyncState == DeviceTopologyNodeSynced && state.SourceGeneration == generation
	}
	nodeForUID := func(uid types.UID, generation uint64) bool {
		for _, state := range nodes {
			if state.Identity.UID == uid && state.SyncState == DeviceTopologyNodeSynced && state.SourceGeneration == generation {
				return true
			}
		}
		return false
	}

	devices := make(map[DeviceKey]TopologyDevice, len(source.Devices))
	for key, device := range source.Devices {
		if nodeFor(device.NodeName, key.OwnerNodeUID, device.SourceGeneration) {
			devices[key] = device
		}
	}
	localDomains := make(map[LocalDomainKey]DeviceDomain, len(source.LocalDomains))
	for key, domain := range source.LocalDomains {
		if nodeFor(domain.NodeName, key.OwnerNodeUID, domain.SourceGeneration) {
			localDomains[key] = domain
		}
	}
	fabrics := make(map[FabricKey]FabricDomain, len(source.Fabrics))
	for key, fabric := range source.Fabrics {
		if !nodeForUID(fabric.OwnerNodeUID, fabric.SourceGeneration) {
			continue
		}
		valid := true
		for _, member := range fabric.Members {
			if !nodeFor(member.NodeName, member.NodeUID, member.SourceGeneration) {
				valid = false
				break
			}
		}
		if valid {
			fabrics[key] = fabric
		}
	}
	localDomainsByClass := make(map[DomainClassKey][]LocalDomainKey, len(source.LocalDomainsByClass))
	for class, keys := range source.LocalDomainsByClass {
		for _, key := range keys {
			if _, found := localDomains[key]; found {
				localDomainsByClass[class] = append(localDomainsByClass[class], key)
			}
		}
	}
	fabricsByClass := make(map[DomainClassKey][]FabricKey, len(source.FabricsByClass))
	for class, keys := range source.FabricsByClass {
		for _, key := range keys {
			if _, found := fabrics[key]; found {
				fabricsByClass[class] = append(fabricsByClass[class], key)
			}
		}
	}
	pendingFabrics := make([]PendingFabric, 0, len(source.PendingFabrics))
	for _, pending := range source.PendingFabrics {
		if hasSyncedNodeUID(nodes, pending.OwnerNodeUID) {
			pendingFabrics = append(pendingFabrics, pending)
		}
	}
	return newDeviceTopologySnapshot(
		nodes, devices, localDomains, fabrics, localDomainsByClass, fabricsByClass, pendingFabrics,
		source.ProviderCapabilitiesKnown, source.SupportedProviderDomainClasses,
	)
}

func hasSyncedNodeUID(nodes map[string]DeviceTopologyNodeState, uid types.UID) bool {
	for _, state := range nodes {
		if state.Identity.UID == uid && state.SyncState == DeviceTopologyNodeSynced {
			return true
		}
	}
	return false
}

// CatalogView is the immutable class lookup consumed by canonicalization.
// It deliberately exposes no mutation or runtime Provider state.
type CatalogView interface {
	HasDomainClass(DomainClassKey) bool
}

// AuthoringSource identifies the only typed API locations accepted by this
// Alpha. It lets the shared canonicalizer enforce selector scope without
// consulting a scheduler or admission implementation.
type AuthoringSource string

const (
	// PodGroupAuthoringSource is PodGroupSpec.DeviceTopology.
	PodGroupAuthoringSource AuthoringSource = "PodGroup"
	// SubGroupAuthoringSource is SubGroupPolicySpec.DeviceTopology.
	SubGroupAuthoringSource AuthoringSource = "SubGroupPolicy"
)

// PolicyFingerprint is a stable digest of canonical user intent. It excludes
// catalog content, resourceVersion, and all runtime topology facts.
type PolicyFingerprint string

// CanonicalLabel is one normalized matchLabels member.
type CanonicalLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// CanonicalLabelSelectorRequirement is one normalized matchExpressions
// member. Values are sorted and exact duplicates are folded.
type CanonicalLabelSelectorRequirement struct {
	Key      string                       `json:"key"`
	Operator metav1.LabelSelectorOperator `json:"operator"`
	Values   []string                     `json:"values,omitempty"`
}

// CanonicalLabelSelector is a map-free stable representation of a Kubernetes
// LabelSelector. The zero value selects every Pod.
type CanonicalLabelSelector struct {
	MatchLabels      []CanonicalLabel                    `json:"matchLabels,omitempty"`
	MatchExpressions []CanonicalLabelSelectorRequirement `json:"matchExpressions,omitempty"`
}

// Empty reports whether the selector has no constraints.
func (s CanonicalLabelSelector) Empty() bool {
	return len(s.MatchLabels) == 0 && len(s.MatchExpressions) == 0
}

// CanonicalDeviceTopologyPolicy is the defaulted, catalog-resolved scheduler
// representation of one user policy.
type CanonicalDeviceTopologyPolicy struct {
	ResourceName corev1.ResourceName              `json:"resourceName"`
	ApplyTo      scheduling.DeviceTopologyApplyTo `json:"applyTo"`
	Mode         scheduling.DeviceTopologyMode    `json:"mode"`
	DomainClass  DomainClassKey                   `json:"domainClass"`
	PodSelector  CanonicalLabelSelector           `json:"podSelector,omitempty"`
}

// CanonicalDeviceTopologySpec contains policies in their stable canonical
// order. It has no pointer to source API objects or mutable catalog data.
type CanonicalDeviceTopologySpec struct {
	Policies []CanonicalDeviceTopologyPolicy `json:"policies"`
}

// Empty reports whether this spec has no policy.
func (s CanonicalDeviceTopologySpec) Empty() bool {
	return len(s.Policies) == 0
}

// Equal compares the semantic, fully canonical form of two policy specs.
func (s CanonicalDeviceTopologySpec) Equal(other CanonicalDeviceTopologySpec) bool {
	return reflect.DeepEqual(s.Policies, other.Policies)
}

// CanonicalizeDeviceTopology defaults, validates, canonicalizes, and resolves
// a typed DeviceTopologySpec. It is intentionally pure: it reads neither a
// SchedulerCache nor live allocation state.
func CanonicalizeDeviceTopology(spec *scheduling.DeviceTopologySpec, source AuthoringSource, catalog CatalogView) (CanonicalDeviceTopologySpec, PolicyFingerprint, field.ErrorList) {
	empty := CanonicalDeviceTopologySpec{Policies: []CanonicalDeviceTopologyPolicy{}}
	if spec == nil || len(spec.Policies) == 0 {
		return empty, fingerprint(empty), nil
	}

	allErrs := field.ErrorList{}
	if source != PodGroupAuthoringSource && source != SubGroupAuthoringSource {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("source"), source, []string{string(PodGroupAuthoringSource), string(SubGroupAuthoringSource)}))
	}

	policies := make([]canonicalPolicyWithPath, 0, len(spec.Policies))
	for index, policy := range spec.Policies {
		path := field.NewPath("policies").Index(index)
		canonical, errs := canonicalizeDeviceTopologyPolicy(policy, source, catalog, path)
		allErrs = append(allErrs, errs...)
		if len(errs) == 0 {
			policies = append(policies, canonicalPolicyWithPath{policy: canonical, path: path})
		}
	}

	if len(allErrs) > 0 {
		return empty, fingerprint(empty), allErrs
	}

	sort.Slice(policies, func(i, j int) bool {
		return compareCanonicalPolicy(policies[i].policy, policies[j].policy) < 0
	})

	unique := make([]canonicalPolicyWithPath, 0, len(policies))
	for _, candidate := range policies {
		if len(unique) > 0 && reflect.DeepEqual(unique[len(unique)-1].policy, candidate.policy) {
			continue
		}
		unique = append(unique, candidate)
	}

	for right := 1; right < len(unique); right++ {
		for left := 0; left < right; left++ {
			first, second := unique[left].policy, unique[right].policy
			if first.ResourceName != second.ResourceName || first.ApplyTo != second.ApplyTo || first.DomainClass.Scope != second.DomainClass.Scope || first.DomainClass.Name == second.DomainClass.Name {
				continue
			}
			if selectorsOverlap(first.PodSelector, second.PodSelector) {
				allErrs = append(allErrs, field.Invalid(unique[right].path.Child("domain", "domainClass"), second.DomainClass.Name,
					"conflicts with an overlapping policy for the same resource, applyTo, and scope"))
			}
		}
	}
	if len(allErrs) > 0 {
		return empty, fingerprint(empty), allErrs
	}

	result := CanonicalDeviceTopologySpec{Policies: make([]CanonicalDeviceTopologyPolicy, len(unique))}
	for i := range unique {
		result.Policies[i] = unique[i].policy
	}
	return result, fingerprint(result), nil
}

type canonicalPolicyWithPath struct {
	policy CanonicalDeviceTopologyPolicy
	path   *field.Path
}

func canonicalizeDeviceTopologyPolicy(policy scheduling.DeviceTopologyPolicy, source AuthoringSource, catalog CatalogView, path *field.Path) (CanonicalDeviceTopologyPolicy, field.ErrorList) {
	var allErrs field.ErrorList
	resourcePath := path.Child("resourceName")
	if !isExtendedResourceName(policy.ResourceName) {
		allErrs = append(allErrs, field.Invalid(resourcePath, policy.ResourceName, "must be a qualified extended resource name"))
	}

	applyTo := policy.ApplyTo
	if applyTo == "" {
		applyTo = scheduling.DeviceTopologyApplyToPod
	}
	if applyTo != scheduling.DeviceTopologyApplyToPod && applyTo != scheduling.DeviceTopologyApplyToGroup {
		allErrs = append(allErrs, field.NotSupported(path.Child("applyTo"), applyTo, []string{string(scheduling.DeviceTopologyApplyToPod), string(scheduling.DeviceTopologyApplyToGroup)}))
	}

	mode := policy.Mode
	if mode == "" {
		mode = scheduling.HardDeviceTopologyMode
	}
	if mode != scheduling.HardDeviceTopologyMode && mode != scheduling.SoftDeviceTopologyMode {
		allErrs = append(allErrs, field.NotSupported(path.Child("mode"), mode, []string{string(scheduling.HardDeviceTopologyMode), string(scheduling.SoftDeviceTopologyMode)}))
	}

	scope := policy.Domain.Scope
	if scope != scheduling.DeviceTopologyDomainScopeNode && scope != scheduling.DeviceTopologyDomainScopeFabric {
		allErrs = append(allErrs, field.NotSupported(path.Child("domain", "scope"), scope, []string{string(scheduling.DeviceTopologyDomainScopeNode), string(scheduling.DeviceTopologyDomainScopeFabric)}))
	}
	if messages := validation.IsDNS1123Label(string(policy.Domain.DomainClass)); len(messages) > 0 {
		allErrs = append(allErrs, field.Invalid(path.Child("domain", "domainClass"), policy.Domain.DomainClass, strings.Join(messages, "; ")))
	}

	selector, selectorErrs := canonicalizeLabelSelector(policy.PodSelector, path.Child("podSelector"))
	allErrs = append(allErrs, selectorErrs...)
	if policy.PodSelector != nil && (source != PodGroupAuthoringSource || applyTo != scheduling.DeviceTopologyApplyToPod) {
		allErrs = append(allErrs, field.Forbidden(path.Child("podSelector"), "is only supported by PodGroup policies with applyTo=Pod"))
	}

	key := DomainClassKey{ResourceName: policy.ResourceName, Scope: scope, Name: policy.Domain.DomainClass}
	if catalog == nil {
		allErrs = append(allErrs, field.Invalid(path.Child("domain", "domainClass"), policy.Domain.DomainClass, "catalog is not ready"))
	} else if len(allErrs) == 0 && !catalog.HasDomainClass(key) {
		allErrs = append(allErrs, field.Invalid(path.Child("domain", "domainClass"), policy.Domain.DomainClass, "is not defined for this resource and scope in the fixed catalog"))
	}

	return CanonicalDeviceTopologyPolicy{
		ResourceName: policy.ResourceName,
		ApplyTo:      applyTo,
		Mode:         mode,
		DomainClass:  key,
		PodSelector:  selector,
	}, allErrs
}

func isExtendedResourceName(resourceName corev1.ResourceName) bool {
	name := string(resourceName)
	return strings.Contains(name, "/") && len(validation.IsQualifiedName(name)) == 0
}

func canonicalizeLabelSelector(selector *metav1.LabelSelector, path *field.Path) (CanonicalLabelSelector, field.ErrorList) {
	if selector == nil {
		return CanonicalLabelSelector{}, nil
	}
	if _, err := metav1.LabelSelectorAsSelector(selector); err != nil {
		return CanonicalLabelSelector{}, field.ErrorList{field.Invalid(path, selector, err.Error())}
	}

	canonical := CanonicalLabelSelector{
		MatchLabels:      make([]CanonicalLabel, 0, len(selector.MatchLabels)),
		MatchExpressions: make([]CanonicalLabelSelectorRequirement, 0, len(selector.MatchExpressions)),
	}
	for key, value := range selector.MatchLabels {
		canonical.MatchLabels = append(canonical.MatchLabels, CanonicalLabel{Key: key, Value: value})
	}
	sort.Slice(canonical.MatchLabels, func(i, j int) bool {
		return canonical.MatchLabels[i].Key < canonical.MatchLabels[j].Key
	})

	for _, requirement := range selector.MatchExpressions {
		values := append([]string(nil), requirement.Values...)
		sort.Strings(values)
		values = compactStrings(values)
		canonical.MatchExpressions = append(canonical.MatchExpressions, CanonicalLabelSelectorRequirement{
			Key:      requirement.Key,
			Operator: requirement.Operator,
			Values:   values,
		})
	}
	sort.Slice(canonical.MatchExpressions, func(i, j int) bool {
		left, right := canonical.MatchExpressions[i], canonical.MatchExpressions[j]
		if left.Key != right.Key {
			return left.Key < right.Key
		}
		if left.Operator != right.Operator {
			return left.Operator < right.Operator
		}
		return strings.Join(left.Values, "\x00") < strings.Join(right.Values, "\x00")
	})

	canonical.MatchExpressions = compactRequirements(canonical.MatchExpressions)
	if canonical.Empty() {
		return CanonicalLabelSelector{}, nil
	}
	return canonical, nil
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

func compactRequirements(requirements []CanonicalLabelSelectorRequirement) []CanonicalLabelSelectorRequirement {
	if len(requirements) < 2 {
		return requirements
	}
	write := 1
	for read := 1; read < len(requirements); read++ {
		if reflect.DeepEqual(requirements[read], requirements[write-1]) {
			continue
		}
		requirements[write] = requirements[read]
		write++
	}
	return requirements[:write]
}

func compareCanonicalPolicy(left, right CanonicalDeviceTopologyPolicy) int {
	leftParts := []string{
		string(left.ResourceName), string(left.ApplyTo), string(left.Mode), string(left.DomainClass.Scope), string(left.DomainClass.Name), selectorKey(left.PodSelector),
	}
	rightParts := []string{
		string(right.ResourceName), string(right.ApplyTo), string(right.Mode), string(right.DomainClass.Scope), string(right.DomainClass.Name), selectorKey(right.PodSelector),
	}
	for index := range leftParts {
		if leftParts[index] < rightParts[index] {
			return -1
		}
		if leftParts[index] > rightParts[index] {
			return 1
		}
	}
	return 0
}

func selectorKey(selector CanonicalLabelSelector) string {
	data, err := json.Marshal(selector)
	if err != nil {
		panic(fmt.Sprintf("canonical label selector cannot marshal: %v", err))
	}
	return string(data)
}

func fingerprint(spec CanonicalDeviceTopologySpec) PolicyFingerprint {
	data, err := json.Marshal(spec)
	if err != nil {
		panic(fmt.Sprintf("canonical device topology cannot marshal: %v", err))
	}
	sum := sha256.Sum256(data)
	return PolicyFingerprint(hex.EncodeToString(sum[:]))
}

// selectorsOverlap reports whether two normalized Kubernetes selectors can
// select at least one common label set. It avoids treating every pair of
// non-identical selectors as conflicting while remaining conservative for the
// conjunction-only LabelSelector language.
func selectorsOverlap(left, right CanonicalLabelSelector) bool {
	constraints := map[string]*labelConstraint{}
	for _, selector := range []CanonicalLabelSelector{left, right} {
		for _, label := range selector.MatchLabels {
			constraint := constraints[label.Key]
			if constraint == nil {
				constraint = &labelConstraint{}
				constraints[label.Key] = constraint
			}
			constraint.requirePresent = true
			constraint.intersectAllowed([]string{label.Value})
		}
		for _, requirement := range selector.MatchExpressions {
			constraint := constraints[requirement.Key]
			if constraint == nil {
				constraint = &labelConstraint{}
				constraints[requirement.Key] = constraint
			}
			switch requirement.Operator {
			case metav1.LabelSelectorOpIn:
				constraint.requirePresent = true
				constraint.intersectAllowed(requirement.Values)
			case metav1.LabelSelectorOpNotIn:
				constraint.addForbidden(requirement.Values)
			case metav1.LabelSelectorOpExists:
				constraint.requirePresent = true
			case metav1.LabelSelectorOpDoesNotExist:
				constraint.requireAbsent = true
			}
		}
	}
	for _, constraint := range constraints {
		if !constraint.satisfiable() {
			return false
		}
	}
	return true
}

type labelConstraint struct {
	requirePresent bool
	requireAbsent  bool
	allowed        map[string]struct{}
	forbidden      map[string]struct{}
}

func (c *labelConstraint) intersectAllowed(values []string) {
	valuesSet := make(map[string]struct{}, len(values))
	for _, value := range values {
		valuesSet[value] = struct{}{}
	}
	if c.allowed == nil {
		c.allowed = valuesSet
		return
	}
	for value := range c.allowed {
		if _, ok := valuesSet[value]; !ok {
			delete(c.allowed, value)
		}
	}
}

func (c *labelConstraint) addForbidden(values []string) {
	if c.forbidden == nil {
		c.forbidden = make(map[string]struct{}, len(values))
	}
	for _, value := range values {
		c.forbidden[value] = struct{}{}
	}
}

func (c *labelConstraint) satisfiable() bool {
	if c.requireAbsent {
		return !c.requirePresent
	}
	if !c.requirePresent {
		return true
	}
	if c.allowed == nil {
		// Label values are an unbounded string domain, so a finite NotIn set
		// always leaves at least one present value available.
		return true
	}
	for value := range c.allowed {
		if _, forbidden := c.forbidden[value]; !forbidden {
			return true
		}
	}
	return false
}
