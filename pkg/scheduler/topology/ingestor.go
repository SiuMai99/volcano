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
	"fmt"
	"sync"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

// FactsIngestor composes the provider generation tracker with pure
// normalization. It is process-scoped provider state, not SchedulerCache: it
// performs no cache-lock work, does not publish snapshots, and has no
// scheduling side effects. The cache-side Provider bridge owns the final
// snapshot publication.
type FactsIngestor struct {
	mu         sync.Mutex
	normalizer *Normalizer
	tracker    *provider.Tracker
}

// NewFactsIngestor creates the narrow bridge from source facts to canonical
// facts. Both catalog and capability are fixed by construction.
func NewFactsIngestor(catalog api.CatalogView, capabilities provider.TopologyProviderCapabilities) (*FactsIngestor, error) {
	normalizer, err := NewNormalizer(catalog, capabilities)
	if err != nil {
		return nil, err
	}
	return &FactsIngestor{normalizer: normalizer, tracker: provider.NewTracker()}, nil
}

// NewAnnotationFactsIngestor constructs the first production Annotation
// Provider bridge from the fixed scheduler catalog. The provider is limited to
// the NVIDIA resource and to the catalog classes declared for that resource;
// it does not infer capabilities from arbitrary catalog entries.
func NewAnnotationFactsIngestor(catalog *Catalog) (*FactsIngestor, error) {
	if catalog == nil {
		return nil, fmt.Errorf("xpu topology catalog is required for Annotation Provider")
	}
	var domainClasses []api.DomainClassKey
	for _, descriptor := range catalog.Descriptors() {
		if descriptor.ResourceName != provider.AnnotationResourceName {
			continue
		}
		for _, class := range descriptor.Classes {
			domainClasses = append(domainClasses, class.Key)
		}
	}
	return NewFactsIngestor(catalog, provider.TopologyProviderCapabilities{
		Identity:      provider.AnnotationProviderIdentity(),
		ResourceName:  provider.AnnotationResourceName,
		DomainClasses: domainClasses,
	})
}

// Apply validates that the provider result still belongs to currentNode, then
// atomically admits it only when node-local and cross-node canonical validation
// both succeed. NodeResourceVersion is retained as provider observation
// metadata, but does not change the Node incarnation identity.
func (i *FactsIngestor) Apply(update provider.ProviderNodeUpdate, currentNode api.NodeIdentity) (provider.ProviderNodeRecord, CanonicalTopology, error) {
	if err := updateMatchesCurrentNode(update, currentNode); err != nil {
		return provider.ProviderNodeRecord{}, CanonicalTopology{}, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	fingerprint, err := i.normalizer.FingerprintUpdate(update)
	if err != nil {
		return provider.ProviderNodeRecord{}, CanonicalTopology{}, err
	}
	var topology CanonicalTopology
	record, err := i.tracker.Apply(update, fingerprint, func(records []provider.ProviderNodeRecord) error {
		// Keep the canonical result produced for the prospective tracker state.
		// Tracker commits the same state immediately after this validator returns,
		// so Apply can return this result without normalizing all records again.
		var err error
		topology, err = i.normalizer.Normalize(records)
		return err
	})
	if err != nil {
		return provider.ProviderNodeRecord{}, CanonicalTopology{}, err
	}
	return record, topology, nil
}

// Snapshot recomputes a fresh caller-owned topology result from the last valid
// provider records. It never returns references into the tracker.
func (i *FactsIngestor) Snapshot() (CanonicalTopology, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.normalizer.Normalize(i.tracker.Records())
}

// Record exposes one deep-copied provider state for focused diagnostics/tests.
func (i *FactsIngestor) Record(key provider.ProviderNodeKey) (provider.ProviderNodeRecord, bool) {
	return i.tracker.Record(key)
}

// SupportedDomainClasses returns the fixed Provider capability declaration
// copied at construction. It exposes neither source facts nor mutable tracker
// state and is safe for SchedulerCache to publish with its paired snapshot.
func (i *FactsIngestor) SupportedDomainClasses() []api.DomainClassKey {
	if i == nil {
		return nil
	}
	return i.normalizer.SupportedDomainClasses()
}

// ForgetNodeObservation retires Provider facts for one Kubernetes Node
// incarnation. SchedulerCache calls this after it has atomically published a
// Pending view for a UID replacement or deletion, so a late result cannot
// poison a later incarnation of the same NodeName. The operation is kept out
// of SchedulerCache's main lock and performs no scheduling action.
func (i *FactsIngestor) ForgetNodeObservation(node api.NodeIdentity) (CanonicalTopology, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.tracker.ForgetNodeObservation(node)
	return i.normalizer.Normalize(i.tracker.Records())
}

func updateMatchesCurrentNode(update provider.ProviderNodeUpdate, current api.NodeIdentity) error {
	if update.NodeName != current.Name || update.NodeUID != current.UID {
		return &FactValidationError{
			Reason:  NodeObservationOutOfDate,
			Problem: "provider update no longer matches the current Node name and UID",
		}
	}
	return nil
}
