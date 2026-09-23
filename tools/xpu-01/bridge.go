// Copyright 2026 The Volcano Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
	assignmentprovider "volcano.sh/volcano/pkg/scheduler/topology/assignment"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

// AssignmentRequest is the scheduler-to-provider handoff for the smallest
// exact-ID bridge. The scheduler owns DeviceKeys; the provider receives them
// as input and must not replace them with a different device.
type AssignmentRequest struct {
	PodUID           string
	Container        api.XPUContainerRef
	NodeName         string
	NodeUID          string
	SourceGeneration uint64
	Contract         NVIDIAContract
}

// BridgeError keeps provider/adapter failures structured so the harness can
// distinguish an unsupported exact-ID path from invalid input or stale facts.
type BridgeError struct {
	Reason string
	Detail string
}

func (e *BridgeError) Error() string {
	return e.Detail
}

func newBridgeError(reason, detail string) error {
	return &BridgeError{Reason: reason, Detail: detail}
}

// NewMockNVIDIAProvider creates a validation-only L0 Provider over nvml-mock
// inventory. Its capabilities intentionally do not satisfy ExactReady.
func NewMockNVIDIAProvider(devices []MockDevice) assignmentprovider.Provider {
	identity := provider.ProviderIdentityRef{
		ProviderID: expectedNVIDIAContract.ProviderID,
		Namespace:  expectedNVIDIAContract.Namespace,
	}
	inventory := make(map[api.DeviceKey]api.TopologyDevice, len(devices))
	for _, device := range devices {
		key := api.DeviceKey{
			ResourceName: corev1.ResourceName(expectedNVIDIAContract.ResourceName),
			OwnerNodeUID: types.UID(strings.TrimSpace(device.NodeUID)),
			ID: api.DeviceID{
				ProviderID: identity.ProviderID,
				Namespace:  identity.Namespace,
				Value:      api.SourceDeviceID(normalizeDeviceID(device.DeviceID)),
			},
		}
		health := api.DeviceUnhealthy
		if device.Healthy {
			health = api.DeviceHealthy
		}
		inventory[key] = api.TopologyDevice{
			Key:              key,
			NodeName:         strings.TrimSpace(device.NodeName),
			Health:           health,
			SourceGeneration: 1,
		}
	}
	return &assignmentprovider.StaticInventoryProvider{
		ProviderIdentity: identity,
		ProfileName:      "mock-nvidia-nvml-validation",
		CapabilitySet: assignmentprovider.Capabilities{
			ValidateSelectedDeviceKeys: true,
		},
		Devices: inventory,
	}
}

// StockNVIDIAProvider models the current native Device Plugin path. It can
// expose capacity and perform kubelet allocation, but it has no contract for
// confirming a scheduler-selected UUID.
type StockNVIDIAProvider struct{}

func (StockNVIDIAProvider) Identity() provider.ProviderIdentityRef {
	return provider.ProviderIdentityRef{ProviderID: expectedNVIDIAContract.ProviderID, Namespace: expectedNVIDIAContract.Namespace}
}

func (StockNVIDIAProvider) Profile() string {
	return "stock-nvidia"
}

func (StockNVIDIAProvider) Capabilities() assignmentprovider.Capabilities {
	return assignmentprovider.Capabilities{}
}

func (StockNVIDIAProvider) ValidateSelected(context.Context, assignmentprovider.SelectedAssignment) (assignmentprovider.Confirmation, error) {
	return assignmentprovider.Confirmation{}, &assignmentprovider.Error{
		Reason: assignmentprovider.AssignmentNotEnforceable,
		Detail: "stock NVIDIA Device Plugin has no scheduler-selected UUID confirmation channel",
	}
}

// AssignmentAdapter is the smallest bridge: it validates the provider
// identity, passes through scheduler-owned keys, and produces the minimum
// assignment annotation value. It does not mutate a Pod or call kubelet.
type AssignmentAdapter struct {
	Provider assignmentprovider.Provider
}

func (a AssignmentAdapter) BuildAssignment(req AssignmentRequest, keys []string) (api.XPUAssignment, string, error) {
	if a.Provider == nil {
		return api.XPUAssignment{}, "", newBridgeError(ReasonAssignmentNotEnforceable, "assignment adapter has no provider")
	}
	if err := validateContract(req.Contract); err != nil {
		return api.XPUAssignment{}, "", newBridgeError(ReasonIdentityMismatch, err.Error())
	}
	identity := a.Provider.Identity()
	if identity.ProviderID != req.Contract.ProviderID || identity.Namespace != req.Contract.Namespace {
		return api.XPUAssignment{}, "", newBridgeError(ReasonIdentityMismatch,
			"provider identity contract does not match scheduler request")
	}

	canonical, err := canonicalizeAssignment(api.XPUAssignment{
		Version: api.XPUAssignmentVersion,
		Assignments: []api.XPUAssignmentEntry{{
			Container:    req.Container,
			ResourceName: corev1.ResourceName(req.Contract.ResourceName),
			Provider:     req.Contract.ProviderID,
			DeviceKeys:   keys,
		}},
	})
	if err != nil {
		return api.XPUAssignment{}, "", newBridgeError(ReasonAssignmentInvalid, err.Error())
	}
	entry := canonical.Assignments[0]
	typedKeys := make([]api.DeviceKey, 0, len(entry.DeviceKeys))
	for _, raw := range entry.DeviceKeys {
		key, err := api.ParseXPUDeviceKey(entry.ResourceName, entry.Provider, req.Contract.Namespace, raw)
		if err != nil {
			return api.XPUAssignment{}, "", newBridgeError(ReasonAssignmentInvalid, err.Error())
		}
		typedKeys = append(typedKeys, key)
	}
	confirmation, err := a.Provider.ValidateSelected(context.Background(), assignmentprovider.SelectedAssignment{
		PodUID:           types.UID(strings.TrimSpace(req.PodUID)),
		Node:             api.NodeIdentity{Name: strings.TrimSpace(req.NodeName), UID: types.UID(strings.TrimSpace(req.NodeUID))},
		Container:        req.Container,
		ResourceName:     corev1.ResourceName(req.Contract.ResourceName),
		DeviceKeys:       typedKeys,
		SourceGeneration: req.SourceGeneration,
	})
	if err != nil {
		return api.XPUAssignment{}, "", err
	}
	if !sameDeviceKeys(confirmation.AcceptedDeviceKeys, typedKeys) {
		return api.XPUAssignment{}, "", newBridgeError(ReasonIdentityMismatch, "provider changed scheduler-selected DeviceKeys")
	}

	serialized, err := api.MarshalXPUAssignment(canonical)
	if err != nil {
		return api.XPUAssignment{}, "", newBridgeError(ReasonAssignmentInvalid,
			"encode assignment: "+err.Error())
	}
	if _, err := ParseAssignment(serialized); err != nil {
		return api.XPUAssignment{}, "", newBridgeError(ReasonAssignmentInvalid,
			"serialized assignment did not round-trip: "+err.Error())
	}
	return canonical, string(serialized), nil
}

func sameDeviceKeys(left, right []api.DeviceKey) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type BridgeResult struct {
	CaseID                    string             `json:"caseID"`
	EvidenceLevel             string             `json:"evidenceLevel"`
	Status                    string             `json:"status"`
	Reason                    string             `json:"reason,omitempty"`
	Capabilities              CapabilityMatrix   `json:"capabilities"`
	ProviderAdapter           string             `json:"providerAdapter,omitempty"`
	SelectedDeviceKey         string             `json:"selectedDeviceKey,omitempty"`
	AssignmentAnnotationKey   string             `json:"assignmentAnnotationKey,omitempty"`
	AssignmentAnnotationValue string             `json:"assignmentAnnotationValue,omitempty"`
	Assignment                *api.XPUAssignment `json:"assignment,omitempty"`
	Details                   []string           `json:"details,omitempty"`
}

// RunBridge runs the positive feasibility path over the collected inventory.
// The same manifest can therefore produce a stock-provider negative result
// and a mock-provider positive contract result without changing the expected
// selected UUID.
func RunBridge(input ProbeInput) BridgeResult {
	caseID := input.CaseID
	if caseID == "" {
		caseID = DefaultCaseID
	}
	level := input.EvidenceLevel
	if level == "" {
		level = "L0"
	}
	providerAdapter := NewMockNVIDIAProvider(input.Devices)
	providerCapabilities := providerAdapter.Capabilities()
	result := BridgeResult{
		CaseID:          caseID,
		EvidenceLevel:   level,
		ProviderAdapter: "mock-nvidia-nvml-validation",
		Capabilities: CapabilityMatrix{
			EnumerateInventory:       true,
			SchedulerSelectedInput:   true,
			ValidateSelectedKey:      providerCapabilities.ValidateSelectedDeviceKeys,
			ConsumeSelectedKey:       providerCapabilities.ConsumeSelectedDeviceKeys,
			ConfirmKubeletDeviceID:   providerCapabilities.ConfirmKubeletDeviceIDs,
			ReconcileRuntimeDeviceID: providerCapabilities.ReconcileRuntimeDeviceIDs,
			ExactReady:               providerCapabilities.ExactReady(),
		},
	}

	if err := validateContract(input.Contract); err != nil {
		return failBridgeResult(result, ReasonIdentityMismatch, err.Error())
	}
	if err := validateInventory(input.Devices); err != nil {
		return failBridgeResult(result, ReasonAssignmentInvalid, err.Error())
	}
	selectedKey, err := CanonicalDeviceKey(input.NodeUID, input.SelectedDeviceID)
	if err != nil {
		return failBridgeResult(result, ReasonAssignmentInvalid, err.Error())
	}

	request := AssignmentRequest{
		PodUID:           input.PodUID,
		Container:        input.Container,
		NodeName:         input.NodeName,
		NodeUID:          input.NodeUID,
		SourceGeneration: input.SourceGeneration,
		Contract:         input.Contract,
	}
	assignment, serialized, err := (AssignmentAdapter{
		Provider: providerAdapter,
	}).BuildAssignment(request, []string{selectedKey})
	if err != nil {
		return failBridgeErrorResult(result, err)
	}

	result.Status = StatusPass
	result.SelectedDeviceKey = selectedKey
	result.AssignmentAnnotationKey = api.XPUAssignmentAnnotationKey
	result.AssignmentAnnotationValue = serialized
	result.Assignment = &assignment
	result.Details = []string{
		"scheduler-selected DeviceKey was passed to the Provider without re-selection",
		"mock NVIDIA Provider confirmed NodeUID and DeviceID membership and health",
		"validation-only mock capabilities did not claim kubelet selected-ID enforcement",
		"Adapter emitted the canonical assignments[] envelope and verified round-trip parsing",
	}
	return result
}

func failBridgeResult(result BridgeResult, reason, detail string) BridgeResult {
	result.Status = StatusFail
	result.Reason = reason
	result.Details = []string{detail}
	return result
}

func failBridgeErrorResult(result BridgeResult, err error) BridgeResult {
	if bridgeErr, ok := err.(*BridgeError); ok {
		return failBridgeResult(result, bridgeErr.Reason, bridgeErr.Detail)
	}
	if providerErr, ok := err.(*assignmentprovider.Error); ok {
		return failBridgeResult(result, string(providerErr.Reason), providerErr.Detail)
	}
	return failBridgeResult(result, ReasonAssignmentInvalid, err.Error())
}
