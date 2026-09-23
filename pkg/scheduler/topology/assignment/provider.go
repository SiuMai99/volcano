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

// Package assignment defines the process-scoped selected-DeviceKey validation
// boundary. It is deliberately separate from topology facts Providers: facts
// publication does not imply that a Provider can enforce an exact assignment.
package assignment

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

// Reason is a stable, low-cardinality assignment failure reason.
type Reason string

const (
	AssignmentNotEnforceable Reason = "XPUAssignmentNotEnforceable"
	AssignmentInvalid        Reason = "XPUAssignmentInvalid"
	IdentityMismatch         Reason = "XPUIdentityContractMismatch"
	TopologyNotReady         Reason = "XPUTopologyDataNotReady"
)

// Error preserves a reason without placing raw Pod or Device identity in a
// metric label.
type Error struct {
	Reason Reason
	Detail string
}

func (e *Error) Error() string {
	return e.Detail
}

// Capabilities declares independently testable assignment abilities. A facts
// Provider that only enumerates devices should expose none of these bits.
type Capabilities struct {
	ValidateSelectedDeviceKeys bool
	ConsumeSelectedDeviceKeys  bool
	ConfirmKubeletDeviceIDs    bool
	ReconcileRuntimeDeviceIDs  bool
}

// Validate rejects capability combinations that would overstate a partial
// bridge as an exact implementation.
func (c Capabilities) Validate() error {
	if c.ConsumeSelectedDeviceKeys && !c.ValidateSelectedDeviceKeys {
		return fmt.Errorf("selected-key consumption requires selected-key validation")
	}
	if c.ConfirmKubeletDeviceIDs && !c.ConsumeSelectedDeviceKeys {
		return fmt.Errorf("kubelet DeviceID confirmation requires selected-key consumption")
	}
	if c.ReconcileRuntimeDeviceIDs && !c.ConfirmKubeletDeviceIDs {
		return fmt.Errorf("runtime DeviceID reconciliation requires kubelet DeviceID confirmation")
	}
	return nil
}

// ExactReady reports only the minimum L1 bridge contract. Runtime-visible UUID
// reconciliation is a separate L2 capability.
func (c Capabilities) ExactReady() bool {
	return c.Validate() == nil && c.ValidateSelectedDeviceKeys && c.ConsumeSelectedDeviceKeys && c.ConfirmKubeletDeviceIDs
}

// SelectedAssignment is the in-process handoff from a future hard planner to
// an assignment Provider. It contains no selection method: DeviceKeys are
// scheduler-owned input and must not be replaced by the Provider.
type SelectedAssignment struct {
	PodUID           types.UID
	Node             api.NodeIdentity
	Container        api.XPUContainerRef
	ResourceName     corev1.ResourceName
	DeviceKeys       []api.DeviceKey
	SourceGeneration uint64
}

// Confirmation records exactly which keys the Provider accepted. Callers must
// compare the full list; a Provider cannot silently return a different device.
type Confirmation struct {
	Profile            string
	AcceptedDeviceKeys []api.DeviceKey
}

// Provider is the smallest process-scoped exact-assignment boundary. It does
// not own scheduling, reservation, release, reconciliation or Pod mutation.
type Provider interface {
	Identity() provider.ProviderIdentityRef
	Profile() string
	Capabilities() Capabilities
	ValidateSelected(context.Context, SelectedAssignment) (Confirmation, error)
}

// ValidateSelectedAssignment checks the generic request shape and Provider
// identity before an implementation consults its own inventory.
func ValidateSelectedAssignment(identity provider.ProviderIdentityRef, request SelectedAssignment) error {
	if strings.TrimSpace(identity.ProviderID) == "" || strings.TrimSpace(identity.Namespace) == "" {
		return assignmentError(IdentityMismatch, "assignment Provider identity is incomplete")
	}
	if request.PodUID == "" {
		return assignmentError(AssignmentInvalid, "Pod UID is empty")
	}
	if strings.TrimSpace(request.Node.Name) == "" || request.Node.UID == "" {
		return assignmentError(AssignmentInvalid, "Node identity is incomplete")
	}
	if request.SourceGeneration == 0 {
		return assignmentError(TopologyNotReady, "source generation must be greater than zero")
	}
	if len(request.DeviceKeys) == 0 {
		return assignmentError(AssignmentInvalid, "selected DeviceKeys are empty")
	}
	if _, err := api.CanonicalizeXPUAssignment(api.XPUAssignment{
		Version: api.XPUAssignmentVersion,
		Assignments: []api.XPUAssignmentEntry{{
			Container:    request.Container,
			ResourceName: request.ResourceName,
			Provider:     identity.ProviderID,
			DeviceKeys:   wireDeviceKeys(request.DeviceKeys),
		}},
	}); err != nil {
		return assignmentError(AssignmentInvalid, err.Error())
	}

	seen := make(map[api.DeviceKey]struct{}, len(request.DeviceKeys))
	for _, key := range request.DeviceKeys {
		if key.ResourceName != request.ResourceName {
			return assignmentError(IdentityMismatch, fmt.Sprintf("selected DeviceKey resource %q does not match request %q", key.ResourceName, request.ResourceName))
		}
		if key.OwnerNodeUID != request.Node.UID {
			return assignmentError(IdentityMismatch, fmt.Sprintf("selected DeviceKey belongs to NodeUID %q, request targets %q", key.OwnerNodeUID, request.Node.UID))
		}
		if key.ID.ProviderID != identity.ProviderID || key.ID.Namespace != identity.Namespace {
			return assignmentError(IdentityMismatch, "selected DeviceKey Provider identity does not match assignment Provider")
		}
		if _, found := seen[key]; found {
			return assignmentError(AssignmentInvalid, "selected DeviceKeys contain a duplicate")
		}
		seen[key] = struct{}{}
	}
	return nil
}

// StaticInventoryProvider is a fixture-only validator for L0 and mock probes.
// It proves identity/membership/health validation, not selected-key consumption
// by kubelet and therefore must not be used to set AssignmentContractReady.
type StaticInventoryProvider struct {
	ProviderIdentity provider.ProviderIdentityRef
	ProfileName      string
	CapabilitySet    Capabilities
	Devices          map[api.DeviceKey]api.TopologyDevice
}

func (p *StaticInventoryProvider) Identity() provider.ProviderIdentityRef {
	if p == nil {
		return provider.ProviderIdentityRef{}
	}
	return p.ProviderIdentity
}

func (p *StaticInventoryProvider) Profile() string {
	if p == nil {
		return ""
	}
	return p.ProfileName
}

func (p *StaticInventoryProvider) Capabilities() Capabilities {
	if p == nil {
		return Capabilities{}
	}
	return p.CapabilitySet
}

func (p *StaticInventoryProvider) ValidateSelected(_ context.Context, request SelectedAssignment) (Confirmation, error) {
	if p == nil {
		return Confirmation{}, assignmentError(AssignmentNotEnforceable, "assignment Provider is nil")
	}
	if err := p.CapabilitySet.Validate(); err != nil {
		return Confirmation{}, assignmentError(AssignmentNotEnforceable, err.Error())
	}
	if strings.TrimSpace(p.ProfileName) == "" {
		return Confirmation{}, assignmentError(AssignmentNotEnforceable, "assignment Provider profile is empty")
	}
	if !p.CapabilitySet.ValidateSelectedDeviceKeys {
		return Confirmation{}, assignmentError(AssignmentNotEnforceable, "Provider cannot validate scheduler-selected DeviceKeys")
	}
	if err := ValidateSelectedAssignment(p.ProviderIdentity, request); err != nil {
		return Confirmation{}, err
	}
	for _, key := range request.DeviceKeys {
		device, found := p.Devices[key]
		if !found {
			return Confirmation{}, assignmentError(IdentityMismatch, "selected DeviceKey is absent from Provider inventory")
		}
		if device.NodeName != request.Node.Name {
			return Confirmation{}, assignmentError(IdentityMismatch, "selected DeviceKey is owned by a different NodeName")
		}
		if device.Health != api.DeviceHealthy {
			return Confirmation{}, assignmentError(TopologyNotReady, "selected DeviceKey is not healthy")
		}
		if device.SourceGeneration != request.SourceGeneration {
			return Confirmation{}, assignmentError(TopologyNotReady, "selected DeviceKey source generation changed")
		}
	}
	return Confirmation{
		Profile:            p.ProfileName,
		AcceptedDeviceKeys: append([]api.DeviceKey(nil), request.DeviceKeys...),
	}, nil
}

func assignmentError(reason Reason, detail string) error {
	return &Error{Reason: reason, Detail: detail}
}

func wireDeviceKeys(keys []api.DeviceKey) []string {
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		wire, err := api.CanonicalXPUDeviceKey(key.OwnerNodeUID, key.ID.Value)
		if err != nil {
			// Preserve an invalid marker so CanonicalizeXPUAssignment returns a
			// structured validation error on the common path.
			wire = ""
		}
		result = append(result, wire)
	}
	return result
}
