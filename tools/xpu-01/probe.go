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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	DefaultCaseID = "xpu01-mock-selected-uuid"

	StatusPass = "Pass"
	StatusFail = "Fail"

	ReasonAssignmentNotEnforceable = "XPUAssignmentNotEnforceable"
	ReasonAssignmentInvalid        = "XPUAssignmentInvalid"
	ReasonIdentityMismatch         = "XPUIdentityContractMismatch"
	ReasonTopologyNotReady         = "XPUTopologyDataNotReady"
)

// NVIDIAContract is the fixed first-provider identity contract from XPU-00 D4.
type NVIDIAContract struct {
	Vendor       string `json:"vendor"`
	ProviderID   string `json:"providerID"`
	Namespace    string `json:"namespace"`
	ResourceName string `json:"resourceName"`
	DiscoveryAPI string `json:"discoveryAPI"`
}

var expectedNVIDIAContract = NVIDIAContract{
	Vendor:       "NVIDIA",
	ProviderID:   "nvidia-nvml-v1",
	Namespace:    "nvidia.com",
	ResourceName: "nvidia.com/gpu",
	DiscoveryAPI: "NVML",
}

// MockDevice is one NVML-visible device in the probe input. It is test input,
// not a scheduler cache or a production topology model.
type MockDevice struct {
	NodeName string `json:"nodeName"`
	NodeUID  string `json:"nodeUID"`
	DeviceID string `json:"deviceID"`
	Healthy  bool   `json:"healthy"`
}

// ProbeInput is intentionally explicit so a run can be replayed from one
// immutable manifest. ProviderCanConfirmSelected models the capability under
// test; it is not an implementation of the NVIDIA Device Plugin protocol.
type ProbeInput struct {
	CaseID                     string              `json:"caseID"`
	EvidenceLevel              string              `json:"evidenceLevel"`
	Contract                   NVIDIAContract      `json:"contract"`
	PodUID                     string              `json:"podUID"`
	Container                  api.XPUContainerRef `json:"container"`
	NodeName                   string              `json:"nodeName"`
	NodeUID                    string              `json:"nodeUID"`
	SourceGeneration           uint64              `json:"sourceGeneration"`
	Devices                    []MockDevice        `json:"devices"`
	SelectedDeviceID           string              `json:"selectedDeviceID"`
	ProviderCanConfirmSelected bool                `json:"providerCanConfirmSelected"`
}

// CapabilityMatrix keeps each exact-assignment claim independent. A profile
// is not exact-ready merely because inventory enumeration or validation works.
type CapabilityMatrix struct {
	EnumerateInventory       bool `json:"enumerateInventory"`
	SchedulerSelectedInput   bool `json:"schedulerSelectedInput"`
	ValidateSelectedKey      bool `json:"validateSelectedKey"`
	ConsumeSelectedKey       bool `json:"consumeSelectedKey"`
	PersistAPIPodAssignment  bool `json:"persistAPIPodAssignment"`
	ConfirmKubeletDeviceID   bool `json:"confirmKubeletDeviceID"`
	ReconcileRuntimeDeviceID bool `json:"reconcileRuntimeDeviceID"`
	ExactReady               bool `json:"exactReady"`
}

// ProbeResult is the structured result written by the CLI and consumed by the
// evidence report. A failed result is still useful evidence when its reason is
// explicit and its inputs are preserved.
type ProbeResult struct {
	CaseID               string             `json:"caseID"`
	EvidenceLevel        string             `json:"evidenceLevel"`
	Status               string             `json:"status"`
	Reason               string             `json:"reason,omitempty"`
	Capabilities         CapabilityMatrix   `json:"capabilities"`
	SelectedDeviceKey    string             `json:"selectedDeviceKey,omitempty"`
	Assignment           *api.XPUAssignment `json:"assignment,omitempty"`
	SerializedAssignment string             `json:"serializedAssignment,omitempty"`
	Details              []string           `json:"details,omitempty"`
}

// CanonicalDeviceKey binds a provider DeviceID to the NodeUID that owns it.
// Device IDs are normalized case-insensitively because NVIDIA UUID spelling is
// not a safe cross-component identity boundary by itself.
func CanonicalDeviceKey(nodeUID, deviceID string) (string, error) {
	return api.CanonicalXPUDeviceKey(types.UID(strings.TrimSpace(nodeUID)), api.SourceDeviceID(normalizeDeviceID(deviceID)))
}

func normalizeDeviceID(deviceID string) string {
	return strings.ToUpper(strings.TrimSpace(deviceID))
}

func canonicalizeAssignment(assignment api.XPUAssignment) (api.XPUAssignment, error) {
	return api.CanonicalizeXPUAssignment(assignment)
}

// ParseAssignment strictly parses and canonicalizes the minimum assignment
// payload. Unknown fields are rejected so a later producer cannot silently
// create an unreviewed contract extension.
func ParseAssignment(data []byte) (api.XPUAssignment, error) {
	return api.ParseXPUAssignment(data)
}

func validateContract(contract NVIDIAContract) error {
	if contract != expectedNVIDIAContract {
		return fmt.Errorf("contract mismatch: got vendor=%q providerID=%q namespace=%q resourceName=%q discoveryAPI=%q",
			contract.Vendor, contract.ProviderID, contract.Namespace, contract.ResourceName, contract.DiscoveryAPI)
	}
	return nil
}

func validateInventory(devices []MockDevice) error {
	seen := make(map[string]struct{}, len(devices))
	for _, device := range devices {
		if strings.TrimSpace(device.NodeName) == "" {
			return fmt.Errorf("device %q has empty node name", device.DeviceID)
		}
		key, err := CanonicalDeviceKey(device.NodeUID, device.DeviceID)
		if err != nil {
			return err
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate device key %q on the inventory", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// RunProbe executes the L0 selected-UUID contract probe. It deliberately
// returns a structured failure instead of guessing a replacement device.
func RunProbe(input ProbeInput) ProbeResult {
	caseID := input.CaseID
	if caseID == "" {
		caseID = DefaultCaseID
	}
	level := input.EvidenceLevel
	if level == "" {
		level = "L0"
	}
	result := ProbeResult{
		CaseID:        caseID,
		EvidenceLevel: level,
		Capabilities: CapabilityMatrix{
			EnumerateInventory:     true,
			SchedulerSelectedInput: true,
			ValidateSelectedKey:    input.ProviderCanConfirmSelected,
		},
	}

	if err := validateContract(input.Contract); err != nil {
		return failResult(result, ReasonIdentityMismatch, err.Error())
	}
	if err := validateInventory(input.Devices); err != nil {
		return failResult(result, ReasonAssignmentInvalid, err.Error())
	}
	if strings.TrimSpace(input.PodUID) == "" {
		return failResult(result, ReasonAssignmentInvalid, "PodUID is empty")
	}
	if input.SourceGeneration == 0 {
		return failResult(result, ReasonTopologyNotReady, "sourceGeneration must be greater than zero")
	}

	selectedID := normalizeDeviceID(input.SelectedDeviceID)
	var selected *MockDevice
	for index := range input.Devices {
		device := &input.Devices[index]
		if strings.TrimSpace(device.NodeName) == strings.TrimSpace(input.NodeName) &&
			strings.TrimSpace(device.NodeUID) == strings.TrimSpace(input.NodeUID) &&
			normalizeDeviceID(device.DeviceID) == selectedID {
			selected = device
			break
		}
	}
	if selected == nil {
		return failResult(result, ReasonIdentityMismatch,
			"selected device is not present on the requested NodeUID")
	}
	if !selected.Healthy {
		return failResult(result, ReasonTopologyNotReady,
			"selected device is not healthy")
	}

	key, err := CanonicalDeviceKey(selected.NodeUID, selected.DeviceID)
	if err != nil {
		return failResult(result, ReasonAssignmentInvalid, err.Error())
	}
	assignment, err := canonicalizeAssignment(api.XPUAssignment{
		Version: api.XPUAssignmentVersion,
		Assignments: []api.XPUAssignmentEntry{{
			Container:    input.Container,
			ResourceName: corev1.ResourceName(expectedNVIDIAContract.ResourceName),
			Provider:     expectedNVIDIAContract.ProviderID,
			DeviceKeys:   []string{key},
		}},
	})
	if err != nil {
		return failResult(result, ReasonAssignmentInvalid, err.Error())
	}
	if !input.ProviderCanConfirmSelected {
		return failResult(result, ReasonAssignmentNotEnforceable,
			"provider did not confirm the scheduler-selected DeviceID")
	}
	serialized, err := api.MarshalXPUAssignment(assignment)
	if err != nil {
		return failResult(result, ReasonAssignmentInvalid, err.Error())
	}
	if _, err := ParseAssignment(serialized); err != nil {
		return failResult(result, ReasonAssignmentInvalid, "serialized assignment did not round-trip: "+err.Error())
	}

	result.Status = StatusPass
	result.SelectedDeviceKey = key
	result.Assignment = &assignment
	result.SerializedAssignment = string(serialized)
	result.Details = []string{
		"provider contract matched NVIDIA/NVML/nvidia.com/gpu",
		"selected DeviceID was present and healthy on the requested NodeUID",
		"assignment payload parsed and canonicalized idempotently",
	}
	return result
}

func failResult(result ProbeResult, reason, detail string) ProbeResult {
	result.Status = StatusFail
	result.Reason = reason
	result.Details = []string{detail}
	return result
}
