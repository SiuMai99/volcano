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

// Package provider defines source-facing xPU topology contracts. Providers
// report immutable facts; they never filter Nodes, score Nodes, choose devices,
// or allocate resources.
package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// ProviderIdentityRef identifies one trusted source implementation. Namespace
// scopes a provider's opaque source IDs; it is not a Kubernetes namespace for
// workload data.
type ProviderIdentityRef struct {
	ProviderID string
	Namespace  string
}

// ProviderNodeKey identifies the state a provider maintains for one resource
// on one concrete Node incarnation. NodeName is intentionally excluded.
type ProviderNodeKey struct {
	ProviderID   string
	Namespace    string
	ResourceName corev1.ResourceName
	NodeUID      types.UID
}

// TopologyProviderCapabilities is the fixed declaration made at provider
// construction. DomainClasses must be an explicit subset of the process-fixed
// catalog; wildcard support is deliberately not representable.
type TopologyProviderCapabilities struct {
	Identity      ProviderIdentityRef
	ResourceName  corev1.ResourceName
	DomainClasses []api.DomainClassKey
}

// Validate checks that the capability has an explicit identity and only names
// classes from the fixed catalog.
func (c TopologyProviderCapabilities) Validate(catalog api.CatalogView) error {
	if strings.TrimSpace(c.Identity.ProviderID) == "" {
		return fmt.Errorf("provider ID is required")
	}
	if strings.TrimSpace(c.Identity.Namespace) == "" {
		return fmt.Errorf("provider namespace is required")
	}
	if !isExtendedResourceName(c.ResourceName) {
		return fmt.Errorf("resource name %q must be a qualified extended resource name", c.ResourceName)
	}
	if catalog == nil {
		return fmt.Errorf("catalog is not ready")
	}
	if len(c.DomainClasses) == 0 {
		return fmt.Errorf("provider %q must declare at least one supported domain class", c.Identity.ProviderID)
	}
	seen := make(map[api.DomainClassKey]struct{}, len(c.DomainClasses))
	for _, class := range c.DomainClasses {
		if class.ResourceName != c.ResourceName {
			return fmt.Errorf("capability class %q has resource %q, want %q", class.Name, class.ResourceName, c.ResourceName)
		}
		if class.Scope != scheduling.DeviceTopologyDomainScopeNode && class.Scope != scheduling.DeviceTopologyDomainScopeFabric {
			return fmt.Errorf("capability class %q has unsupported scope %q", class.Name, class.Scope)
		}
		if _, exists := seen[class]; exists {
			return fmt.Errorf("capability class %q/%q is duplicated", class.Scope, class.Name)
		}
		seen[class] = struct{}{}
		if !catalog.HasDomainClass(class) {
			return fmt.Errorf("capability class %q/%q is not in the fixed catalog", class.Scope, class.Name)
		}
	}
	return nil
}

// ProviderNodeSyncState describes whether a source has completed a valid
// observation for exactly one ProviderNodeKey.
type ProviderNodeSyncState string

const (
	// ProviderNodePending has not received a valid replacement or clear.
	ProviderNodePending ProviderNodeSyncState = "Pending"
	// ProviderNodeSynced received a valid replacement or explicit clear.
	ProviderNodeSynced ProviderNodeSyncState = "Synced"
)

// ProviderUpdateOperation determines how a provider observation changes its
// complete source view for one ProviderNodeKey.
type ProviderUpdateOperation string

const (
	// ReplaceFacts replaces the whole fact set for the source key.
	ReplaceFacts ProviderUpdateOperation = "ReplaceFacts"
	// ClearFacts records that the provider checked and found no inventory.
	ClearFacts ProviderUpdateOperation = "ClearFacts"
)

// DeviceFact is a source-facing device observation. ID is only meaningful
// together with the provider identity, resource, and NodeUID from its update.
type DeviceFact struct {
	ID     api.SourceDeviceID `json:"id"`
	Health api.DeviceHealth   `json:"health"`
}

// LocalDomainFact is one explicit node-local topology domain. DomainIDs allow
// a source to form an explicit hierarchy; Normalizer validates it as acyclic
// and derives the transitive effective device set.
type LocalDomainFact struct {
	ID          api.SourceDomainID                   `json:"id"`
	DomainClass scheduling.DeviceTopologyDomainClass `json:"domainClass"`
	DeviceIDs   []api.SourceDeviceID                 `json:"deviceIDs,omitempty"`
	DomainIDs   []api.SourceDomainID                 `json:"domainIDs,omitempty"`
}

// FabricMemberFact references local domains published by a member Node. It
// contains NodeName only because the current NodeUID and source generation
// are resolved from that Node's ProviderNodeKey during normalization.
type FabricMemberFact struct {
	NodeName         string               `json:"node"`
	SourceGeneration uint64               `json:"sourceGeneration"`
	LocalDomainIDs   []api.SourceDomainID `json:"localDomains"`
}

// FabricFact is a complete single-owner declaration. It is not an incremental
// membership patch and cannot be inferred from HyperNode or network facts.
type FabricFact struct {
	ID          api.SourceFabricID                   `json:"id"`
	DomainClass scheduling.DeviceTopologyDomainClass `json:"domainClass"`
	OwnerNode   string                               `json:"ownerNode"`
	Members     []FabricMemberFact                   `json:"members"`
}

// NodeTopologyFacts is the strict source payload after provider-specific
// parsing. Every payload describes exactly one extended resource.
type NodeTopologyFacts struct {
	ResourceName corev1.ResourceName `json:"resourceName"`
	Devices      []DeviceFact        `json:"devices"`
	LocalDomains []LocalDomainFact   `json:"localDomains"`
	Fabrics      []FabricFact        `json:"fabrics,omitempty"`
}

// ProviderNodeUpdate is one observation from a provider. NodeResourceVersion
// is opaque source-correlation metadata; it is not part of ProviderNodeKey and
// must never be used as a topology identity or ordered numerically.
type ProviderNodeUpdate struct {
	ProviderID          string
	IdentityNamespace   string
	ResourceName        corev1.ResourceName
	NodeName            string
	NodeUID             types.UID
	NodeResourceVersion string
	SourceGeneration    uint64
	ObservedAt          time.Time
	FreshUntil          time.Time
	Operation           ProviderUpdateOperation
	Facts               *NodeTopologyFacts
}

// Key returns the identity whose freshness and generation this update changes.
func (u ProviderNodeUpdate) Key() ProviderNodeKey {
	return ProviderNodeKey{
		ProviderID:   u.ProviderID,
		Namespace:    u.IdentityNamespace,
		ResourceName: u.ResourceName,
		NodeUID:      u.NodeUID,
	}
}

// ValidateShape checks contract fields that do not require catalog or topology
// graph knowledge. Semantic source validation belongs to Normalizer.
func (u ProviderNodeUpdate) ValidateShape() error {
	if strings.TrimSpace(u.ProviderID) == "" {
		return fmt.Errorf("provider ID is required")
	}
	if strings.TrimSpace(u.IdentityNamespace) == "" {
		return fmt.Errorf("provider namespace is required")
	}
	if !isExtendedResourceName(u.ResourceName) {
		return fmt.Errorf("resource name %q must be a qualified extended resource name", u.ResourceName)
	}
	if strings.TrimSpace(u.NodeName) == "" || u.NodeUID == "" {
		return fmt.Errorf("node name and UID are required")
	}
	if u.SourceGeneration == 0 {
		return fmt.Errorf("source generation must be greater than zero")
	}
	switch u.Operation {
	case ReplaceFacts:
		if u.Facts == nil {
			return fmt.Errorf("ReplaceFacts requires facts")
		}
	case ClearFacts:
		if u.Facts != nil {
			return fmt.Errorf("ClearFacts must not include facts")
		}
	default:
		return fmt.Errorf("operation %q is unsupported", u.Operation)
	}
	return nil
}

// ProviderNodeRecord is an immutable-by-copy tracker entry. Facts and nested
// slices are copied at the tracker boundary so callers cannot mutate history.
type ProviderNodeRecord struct {
	Key                ProviderNodeKey
	State              ProviderNodeSyncState
	Update             ProviderNodeUpdate
	ContentFingerprint string
}

// ApplyValidator is run while Tracker holds its state lock, against a copy of
// the prospective complete state. Returning an error aborts the update, so an
// invalid cross-node Fabric declaration cannot replace prior valid facts.
type ApplyValidator func([]ProviderNodeRecord) error

// Tracker owns generation, freshness, and Pending/Synced transitions. It has
// no scheduler cache, device allocator, or Node selection logic.
type Tracker struct {
	mu      sync.RWMutex
	records map[ProviderNodeKey]ProviderNodeRecord
}

// NewTracker creates a per-provider lifecycle tracker.
func NewTracker() *Tracker {
	return &Tracker{records: make(map[ProviderNodeKey]ProviderNodeRecord)}
}

// Apply accepts one already-normalized source update. The content fingerprint
// must describe only canonical topology content, not resourceVersion or
// observation times, allowing same-generation heartbeats to refresh freshness.
func (t *Tracker) Apply(update ProviderNodeUpdate, contentFingerprint string, validate ApplyValidator) (ProviderNodeRecord, error) {
	if err := update.ValidateShape(); err != nil {
		return ProviderNodeRecord{}, err
	}
	if strings.TrimSpace(contentFingerprint) == "" {
		return ProviderNodeRecord{}, fmt.Errorf("content fingerprint is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := update.Key()
	current, found := t.records[key]
	if found {
		switch {
		case update.SourceGeneration < current.Update.SourceGeneration:
			return ProviderNodeRecord{}, fmt.Errorf("source generation %d is older than current generation %d for %#v", update.SourceGeneration, current.Update.SourceGeneration, key)
		case update.SourceGeneration == current.Update.SourceGeneration && contentFingerprint != current.ContentFingerprint:
			return ProviderNodeRecord{}, fmt.Errorf("source generation %d changes topology content for %#v", update.SourceGeneration, key)
		}
	}

	next := ProviderNodeRecord{
		Key:                key,
		State:              ProviderNodeSynced,
		Update:             cloneUpdate(update),
		ContentFingerprint: contentFingerprint,
	}
	prospective := cloneRecords(t.records)
	prospective[key] = next
	if validate != nil {
		if err := validate(recordsFromMap(prospective)); err != nil {
			return ProviderNodeRecord{}, err
		}
	}
	t.records[key] = next
	return cloneRecord(next), nil
}

// Record returns a copy of one state entry.
func (t *Tracker) Record(key ProviderNodeKey) (ProviderNodeRecord, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	record, found := t.records[key]
	return cloneRecord(record), found
}

// State reports Pending until this exact ProviderNodeKey has received a valid
// replacement or explicit clear. Callers must not transfer this readiness to a
// different NodeUID with the same NodeName.
func (t *Tracker) State(key ProviderNodeKey) ProviderNodeSyncState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	record, found := t.records[key]
	if !found {
		return ProviderNodePending
	}
	return record.State
}

// Records returns deterministic, deep-copied tracker state suitable for pure
// normalization. A ClearFacts entry remains Synced but has nil Facts.
func (t *Tracker) Records() []ProviderNodeRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return recordsFromMap(t.records)
}

// ForgetNodeObservation removes all source records for one retired Kubernetes
// Node incarnation. ResourceVersion is deliberately ignored: a same-UID Node
// metadata update must retain its topology facts, while Node UID retirement
// removes every provider record for that incarnation.
func (t *Tracker) ForgetNodeObservation(node api.NodeIdentity) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	removed := false
	for key, record := range t.records {
		if key.NodeUID == node.UID && record.Update.NodeName == node.Name {
			delete(t.records, key)
			removed = true
		}
	}
	return removed
}

// ContentFingerprint returns a stable digest for a caller-owned canonical
// value. It is provided for test Fixtures and provider implementations that
// have already sorted their source-independent content.
func ContentFingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal provider topology content: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func recordsFromMap(records map[ProviderNodeKey]ProviderNodeRecord) []ProviderNodeRecord {
	result := make([]ProviderNodeRecord, 0, len(records))
	for _, record := range records {
		result = append(result, cloneRecord(record))
	}
	sort.Slice(result, func(i, j int) bool {
		return providerNodeKeyString(result[i].Key) < providerNodeKeyString(result[j].Key)
	})
	return result
}

func cloneRecords(records map[ProviderNodeKey]ProviderNodeRecord) map[ProviderNodeKey]ProviderNodeRecord {
	result := make(map[ProviderNodeKey]ProviderNodeRecord, len(records))
	for key, record := range records {
		result[key] = cloneRecord(record)
	}
	return result
}

func cloneRecord(record ProviderNodeRecord) ProviderNodeRecord {
	record.Update = cloneUpdate(record.Update)
	return record
}

func cloneUpdate(update ProviderNodeUpdate) ProviderNodeUpdate {
	if update.Facts == nil {
		return update
	}
	facts := *update.Facts
	facts.Devices = append([]DeviceFact(nil), facts.Devices...)
	facts.LocalDomains = make([]LocalDomainFact, len(update.Facts.LocalDomains))
	for index, domain := range update.Facts.LocalDomains {
		domain.DeviceIDs = append([]api.SourceDeviceID(nil), domain.DeviceIDs...)
		domain.DomainIDs = append([]api.SourceDomainID(nil), domain.DomainIDs...)
		facts.LocalDomains[index] = domain
	}
	facts.Fabrics = make([]FabricFact, len(update.Facts.Fabrics))
	for index, fabric := range update.Facts.Fabrics {
		members := fabric.Members
		fabric.Members = make([]FabricMemberFact, len(members))
		for memberIndex, member := range members {
			member.LocalDomainIDs = append([]api.SourceDomainID(nil), member.LocalDomainIDs...)
			fabric.Members[memberIndex] = member
		}
		facts.Fabrics[index] = fabric
	}
	update.Facts = &facts
	return update
}

func providerNodeKeyString(key ProviderNodeKey) string {
	return strings.Join([]string{key.ProviderID, key.Namespace, string(key.ResourceName), string(key.NodeUID)}, "\x00")
}

func isExtendedResourceName(resourceName corev1.ResourceName) bool {
	name := string(resourceName)
	return strings.Contains(name, "/") && len(validation.IsQualifiedName(name)) == 0
}
