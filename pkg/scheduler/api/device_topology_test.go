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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

type testCatalog map[DomainClassKey]struct{}

func (c testCatalog) HasDomainClass(key DomainClassKey) bool {
	_, found := c[key]
	return found
}

var topologyTestCatalog = testCatalog{
	{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}:       {},
	{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeFabric, Name: "scale-up-fabric"}:    {},
	{ResourceName: "huawei.com/ascend910", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}: {},
}

func TestCanonicalizeDeviceTopologyDefaultsSortsAndDeduplicates(t *testing.T) {
	input := &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{
		{
			ResourceName: "nvidia.com/gpu",
			Domain: scheduling.DeviceTopologyDomainSelector{
				Scope:       scheduling.DeviceTopologyDomainScopeNode,
				DomainClass: "local-scale-up",
			},
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"team": "ml", "app": "trainer"},
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "role",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"worker", "worker", "chief"},
				}},
			},
		},
		{
			ResourceName: "nvidia.com/gpu",
			ApplyTo:      scheduling.DeviceTopologyApplyToGroup,
			Mode:         scheduling.SoftDeviceTopologyMode,
			Domain: scheduling.DeviceTopologyDomainSelector{
				Scope:       scheduling.DeviceTopologyDomainScopeFabric,
				DomainClass: "scale-up-fabric",
			},
		},
		// This is semantically identical to the first policy after defaults
		// and selector normalization, and must be folded.
		{
			ResourceName: "nvidia.com/gpu",
			Domain: scheduling.DeviceTopologyDomainSelector{
				Scope:       scheduling.DeviceTopologyDomainScopeNode,
				DomainClass: "local-scale-up",
			},
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "trainer", "team": "ml"},
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "role",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"chief", "worker"},
				}},
			},
		},
	}}

	canonical, fingerprint, errs := CanonicalizeDeviceTopology(input, PodGroupAuthoringSource, topologyTestCatalog)
	if len(errs) != 0 {
		t.Fatalf("CanonicalizeDeviceTopology() errors = %v", errs)
	}
	if len(canonical.Policies) != 2 {
		t.Fatalf("canonical policies = %#v, want 2 entries", canonical.Policies)
	}
	if got := canonical.Policies[0].DomainClass.Scope; got != scheduling.DeviceTopologyDomainScopeFabric {
		t.Fatalf("first sorted scope = %q, want Fabric", got)
	}
	policy := canonical.Policies[1]
	if policy.ApplyTo != scheduling.DeviceTopologyApplyToPod || policy.Mode != scheduling.HardDeviceTopologyMode {
		t.Fatalf("defaults = applyTo %q, mode %q; want Pod/hard", policy.ApplyTo, policy.Mode)
	}
	if got, want := policy.PodSelector.MatchLabels, []CanonicalLabel{{Key: "app", Value: "trainer"}, {Key: "team", Value: "ml"}}; !policySliceEqual(got, want) {
		t.Fatalf("normalized labels = %#v, want %#v", got, want)
	}
	if got, want := policy.PodSelector.MatchExpressions[0].Values, []string{"chief", "worker"}; !stringSliceEqual(got, want) {
		t.Fatalf("normalized values = %#v, want %#v", got, want)
	}

	equivalent := &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{input.Policies[1], input.Policies[2], input.Policies[0]}}
	canonicalEquivalent, equivalentFingerprint, errs := CanonicalizeDeviceTopology(equivalent, PodGroupAuthoringSource, topologyTestCatalog)
	if len(errs) != 0 {
		t.Fatalf("equivalent canonicalization errors = %v", errs)
	}
	if !canonical.Equal(canonicalEquivalent) || fingerprint != equivalentFingerprint {
		t.Fatalf("canonicalization/fingerprint are not stable: %#v/%s != %#v/%s", canonical, fingerprint, canonicalEquivalent, equivalentFingerprint)
	}
}

func TestCanonicalizeDeviceTopologyClassIdentityIncludesResourceAndScope(t *testing.T) {
	spec := &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{
		{
			ResourceName: "nvidia.com/gpu",
			Domain:       scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
		},
		{
			ResourceName: "huawei.com/ascend910",
			Domain:       scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
		},
	}}
	canonical, _, errs := CanonicalizeDeviceTopology(spec, PodGroupAuthoringSource, topologyTestCatalog)
	if len(errs) != 0 {
		t.Fatalf("CanonicalizeDeviceTopology() errors = %v", errs)
	}
	if len(canonical.Policies) != 2 || canonical.Policies[0].DomainClass == canonical.Policies[1].DomainClass {
		t.Fatalf("same class name must remain distinct by resource: %#v", canonical.Policies)
	}
}

func TestCanonicalizeDeviceTopologyRejectsOverlappingClassConflict(t *testing.T) {
	spec := &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{
		{
			ResourceName: "nvidia.com/gpu",
			Domain:       scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
			PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"team": "ml"}},
		},
		{
			ResourceName: "nvidia.com/gpu",
			Domain:       scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "different-class"},
			PodSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: "team", Operator: metav1.LabelSelectorOpIn, Values: []string{"ml", "research"},
			}}},
		},
	}}
	catalog := testCatalog{}
	for key := range topologyTestCatalog {
		catalog[key] = struct{}{}
	}
	catalog[DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "different-class"}] = struct{}{}

	canonical, _, errs := CanonicalizeDeviceTopology(spec, PodGroupAuthoringSource, catalog)
	if len(errs) == 0 {
		t.Fatal("overlapping policies with different classes must be rejected")
	}
	if !canonical.Empty() {
		t.Fatalf("invalid policy must not leave a partial canonical result: %#v", canonical)
	}
}

func TestCanonicalizeDeviceTopologyAllowsDisjointClassSelectors(t *testing.T) {
	spec := &scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{
		{
			ResourceName: "nvidia.com/gpu",
			Domain:       scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"},
			PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"team": "ml"}},
		},
		{
			ResourceName: "nvidia.com/gpu",
			Domain:       scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "different-class"},
			PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"team": "research"}},
		},
	}}
	catalog := testCatalog{}
	for key := range topologyTestCatalog {
		catalog[key] = struct{}{}
	}
	catalog[DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "different-class"}] = struct{}{}

	canonical, _, errs := CanonicalizeDeviceTopology(spec, PodGroupAuthoringSource, catalog)
	if len(errs) != 0 {
		t.Fatalf("disjoint selectors should not conflict: %v", errs)
	}
	if len(canonical.Policies) != 2 {
		t.Fatalf("canonical policies = %#v, want two disjoint policies", canonical.Policies)
	}
}

func TestCanonicalizeDeviceTopologyValidation(t *testing.T) {
	tests := []struct {
		name   string
		source AuthoringSource
		policy scheduling.DeviceTopologyPolicy
	}{
		{
			name: "native resource", source: PodGroupAuthoringSource,
			policy: scheduling.DeviceTopologyPolicy{ResourceName: corev1.ResourceCPU, Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"}},
		},
		{
			name: "invalid mode", source: PodGroupAuthoringSource,
			policy: scheduling.DeviceTopologyPolicy{ResourceName: "nvidia.com/gpu", Mode: "best-effort", Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"}},
		},
		{
			name: "invalid scope", source: PodGroupAuthoringSource,
			policy: scheduling.DeviceTopologyPolicy{ResourceName: "nvidia.com/gpu", Domain: scheduling.DeviceTopologyDomainSelector{Scope: "Rack", DomainClass: "local-scale-up"}},
		},
		{
			name: "unknown class", source: PodGroupAuthoringSource,
			policy: scheduling.DeviceTopologyPolicy{ResourceName: "nvidia.com/gpu", Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "unknown"}},
		},
		{
			name: "group selector", source: PodGroupAuthoringSource,
			policy: scheduling.DeviceTopologyPolicy{ResourceName: "nvidia.com/gpu", ApplyTo: scheduling.DeviceTopologyApplyToGroup, Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"}, PodSelector: &metav1.LabelSelector{}},
		},
		{
			name: "subgroup selector", source: SubGroupAuthoringSource,
			policy: scheduling.DeviceTopologyPolicy{ResourceName: "nvidia.com/gpu", Domain: scheduling.DeviceTopologyDomainSelector{Scope: scheduling.DeviceTopologyDomainScopeNode, DomainClass: "local-scale-up"}, PodSelector: &metav1.LabelSelector{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, errs := CanonicalizeDeviceTopology(&scheduling.DeviceTopologySpec{Policies: []scheduling.DeviceTopologyPolicy{tt.policy}}, tt.source, topologyTestCatalog)
			if len(errs) == 0 {
				t.Fatal("CanonicalizeDeviceTopology() unexpectedly accepted invalid policy")
			}
		})
	}
}

func TestCanonicalKeysAreNodeUIDSafe(t *testing.T) {
	deviceID := DeviceID{ProviderID: "nvidia-nvml-v1", Namespace: "nvidia.com", Value: "GPU-0"}
	first := DeviceKey{ResourceName: "nvidia.com/gpu", OwnerNodeUID: types.UID("node-uid-a"), ID: deviceID}
	second := DeviceKey{ResourceName: "nvidia.com/gpu", OwnerNodeUID: types.UID("node-uid-b"), ID: deviceID}
	if first == second {
		t.Fatal("same source ID on a replacement Node must not share DeviceKey identity")
	}

	domainID := DomainID{ProviderID: "nvidia-nvml-v1", Namespace: "nvidia.com", Value: "domain-0"}
	firstDomain := LocalDomainKey{ResourceName: "nvidia.com/gpu", OwnerNodeUID: first.OwnerNodeUID, ID: domainID}
	secondDomain := LocalDomainKey{ResourceName: "nvidia.com/gpu", OwnerNodeUID: second.OwnerNodeUID, ID: domainID}
	if firstDomain == secondDomain {
		t.Fatal("same local domain source ID on a replacement Node must not share LocalDomainKey identity")
	}
}

func policySliceEqual(left, right []CanonicalLabel) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func stringSliceEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
