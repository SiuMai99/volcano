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
	"encoding/json"
	"fmt"
	"strings"
)

const AssignmentAnnotationKey = "volcano.sh/xpu-assignment"

// AssignmentRequest is the scheduler-to-provider handoff for the smallest
// exact-ID bridge. The scheduler owns DeviceKeys; the provider receives them
// as input and must not replace them with a different device.
type AssignmentRequest struct {
	NodeName string
	NodeUID  string
	Contract NVIDIAContract
}

// AssignmentProvider is the minimal provider contract exercised by XPU-01.
// It intentionally contains no scheduling or device-selection method.
type AssignmentProvider interface {
	IdentityContract() NVIDIAContract
	ValidateAssignment(req AssignmentRequest, keys []string) error
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

// MockNVIDIAProvider validates exact DeviceKeys against the inventory captured
// from nvml-mock. It is a provider test double, not a replacement Device
// Plugin, and it does not select a device on behalf of the scheduler.
type MockNVIDIAProvider struct {
	contract NVIDIAContract
	devices  []MockDevice
}

func NewMockNVIDIAProvider(devices []MockDevice) *MockNVIDIAProvider {
	return &MockNVIDIAProvider{
		contract: expectedNVIDIAContract,
		devices:  append([]MockDevice(nil), devices...),
	}
}

func (p *MockNVIDIAProvider) IdentityContract() NVIDIAContract {
	return p.contract
}

func (p *MockNVIDIAProvider) ValidateAssignment(req AssignmentRequest, keys []string) error {
	if p == nil {
		return newBridgeError(ReasonAssignmentNotEnforceable, "provider is nil")
	}
	if p.contract != req.Contract {
		return newBridgeError(ReasonIdentityMismatch, "provider identity contract does not match request")
	}
	if len(keys) == 0 {
		return newBridgeError(ReasonAssignmentInvalid, "provider received no selected DeviceKeys")
	}

	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		nodeUID, deviceID, err := splitCanonicalDeviceKey(key)
		if err != nil {
			return newBridgeError(ReasonAssignmentInvalid, err.Error())
		}
		if nodeUID != strings.TrimSpace(req.NodeUID) {
			return newBridgeError(ReasonIdentityMismatch,
				fmt.Sprintf("selected DeviceKey belongs to NodeUID %q, request targets %q", nodeUID, req.NodeUID))
		}
		if _, exists := seen[key]; exists {
			return newBridgeError(ReasonAssignmentInvalid, fmt.Sprintf("duplicate selected DeviceKey %q", key))
		}
		seen[key] = struct{}{}

		found := false
		for _, device := range p.devices {
			if strings.TrimSpace(device.NodeUID) != nodeUID || normalizeDeviceID(device.DeviceID) != deviceID {
				continue
			}
			if strings.TrimSpace(device.NodeName) != strings.TrimSpace(req.NodeName) {
				return newBridgeError(ReasonIdentityMismatch,
					fmt.Sprintf("DeviceKey %q is not owned by requested NodeName %q", key, req.NodeName))
			}
			if !device.Healthy {
				return newBridgeError(ReasonTopologyNotReady,
					fmt.Sprintf("selected DeviceKey %q is not healthy", key))
			}
			found = true
			break
		}
		if !found {
			return newBridgeError(ReasonIdentityMismatch,
				fmt.Sprintf("selected DeviceKey %q is absent from provider inventory", key))
		}
	}
	return nil
}

// StockNVIDIAProvider models the current native Device Plugin path. It can
// expose capacity and perform kubelet allocation, but it has no contract for
// confirming a scheduler-selected UUID.
type StockNVIDIAProvider struct{}

func (StockNVIDIAProvider) IdentityContract() NVIDIAContract {
	return expectedNVIDIAContract
}

func (StockNVIDIAProvider) ValidateAssignment(AssignmentRequest, []string) error {
	return newBridgeError(ReasonAssignmentNotEnforceable,
		"stock NVIDIA Device Plugin has no scheduler-selected UUID confirmation channel")
}

// AssignmentAdapter is the smallest bridge: it validates the provider
// identity, passes through scheduler-owned keys, and produces the minimum
// assignment annotation value. It does not mutate a Pod or call kubelet.
type AssignmentAdapter struct {
	Provider AssignmentProvider
}

func (a AssignmentAdapter) BuildAssignment(req AssignmentRequest, keys []string) (Assignment, string, error) {
	if a.Provider == nil {
		return Assignment{}, "", newBridgeError(ReasonAssignmentNotEnforceable, "assignment adapter has no provider")
	}
	if err := validateContract(req.Contract); err != nil {
		return Assignment{}, "", newBridgeError(ReasonIdentityMismatch, err.Error())
	}
	if a.Provider.IdentityContract() != req.Contract {
		return Assignment{}, "", newBridgeError(ReasonIdentityMismatch,
			"provider identity contract does not match scheduler request")
	}

	canonical, err := canonicalizeAssignment(Assignment{
		Version:      1,
		ResourceName: req.Contract.ResourceName,
		Provider:     req.Contract.ProviderID,
		DeviceKeys:   keys,
	})
	if err != nil {
		return Assignment{}, "", newBridgeError(ReasonAssignmentInvalid, err.Error())
	}
	if err := a.Provider.ValidateAssignment(req, canonical.DeviceKeys); err != nil {
		return Assignment{}, "", err
	}

	serialized, err := json.Marshal(canonical)
	if err != nil {
		return Assignment{}, "", newBridgeError(ReasonAssignmentInvalid,
			"encode assignment: "+err.Error())
	}
	if _, err := ParseAssignment(serialized); err != nil {
		return Assignment{}, "", newBridgeError(ReasonAssignmentInvalid,
			"serialized assignment did not round-trip: "+err.Error())
	}
	return canonical, string(serialized), nil
}

func splitCanonicalDeviceKey(key string) (string, string, error) {
	key = strings.TrimSpace(key)
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid canonical DeviceKey %q", key)
	}
	canonical, err := CanonicalDeviceKey(parts[0], parts[1])
	if err != nil {
		return "", "", fmt.Errorf("invalid canonical DeviceKey %q: %w", key, err)
	}
	if canonical != key {
		return "", "", fmt.Errorf("DeviceKey %q is not canonical; want %q", key, canonical)
	}
	return parts[0], normalizeDeviceID(parts[1]), nil
}

type BridgeResult struct {
	CaseID                    string      `json:"caseID"`
	EvidenceLevel             string      `json:"evidenceLevel"`
	Status                    string      `json:"status"`
	Reason                    string      `json:"reason,omitempty"`
	ProviderAdapter           string      `json:"providerAdapter,omitempty"`
	SelectedDeviceKey         string      `json:"selectedDeviceKey,omitempty"`
	AssignmentAnnotationKey   string      `json:"assignmentAnnotationKey,omitempty"`
	AssignmentAnnotationValue string      `json:"assignmentAnnotationValue,omitempty"`
	Assignment                *Assignment `json:"assignment,omitempty"`
	Details                   []string    `json:"details,omitempty"`
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
	result := BridgeResult{
		CaseID:          caseID,
		EvidenceLevel:   level,
		ProviderAdapter: "mock-nvidia-nvml-exact",
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
		NodeName: input.NodeName,
		NodeUID:  input.NodeUID,
		Contract: input.Contract,
	}
	assignment, serialized, err := (AssignmentAdapter{
		Provider: NewMockNVIDIAProvider(input.Devices),
	}).BuildAssignment(request, []string{selectedKey})
	if err != nil {
		return failBridgeErrorResult(result, err)
	}

	result.Status = StatusPass
	result.SelectedDeviceKey = selectedKey
	result.AssignmentAnnotationKey = AssignmentAnnotationKey
	result.AssignmentAnnotationValue = serialized
	result.Assignment = &assignment
	result.Details = []string{
		"scheduler-selected DeviceKey was passed to the Provider without re-selection",
		"mock NVIDIA Provider confirmed exact NodeUID and DeviceID membership and health",
		"Adapter emitted the minimum volcano.sh/xpu-assignment value and verified round-trip parsing",
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
	return failBridgeResult(result, ReasonAssignmentInvalid, err.Error())
}
