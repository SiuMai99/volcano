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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
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
	CaseID                     string         `json:"caseID"`
	EvidenceLevel              string         `json:"evidenceLevel"`
	Contract                   NVIDIAContract `json:"contract"`
	NodeName                   string         `json:"nodeName"`
	NodeUID                    string         `json:"nodeUID"`
	Devices                    []MockDevice   `json:"devices"`
	SelectedDeviceID           string         `json:"selectedDeviceID"`
	ProviderCanConfirmSelected bool           `json:"providerCanConfirmSelected"`
}

// Assignment is the minimum scheduler-owned assignment payload defined by
// XPU-00. Keep this type small: Domain/Fabric/Group fields are derived facts,
// not part of the Alpha assignment contract.
type Assignment struct {
	Version      int      `json:"version"`
	ResourceName string   `json:"resourceName"`
	Provider     string   `json:"provider"`
	DeviceKeys   []string `json:"deviceKeys"`
}

// ProbeResult is the structured result written by the CLI and consumed by the
// evidence report. A failed result is still useful evidence when its reason is
// explicit and its inputs are preserved.
type ProbeResult struct {
	CaseID               string      `json:"caseID"`
	EvidenceLevel        string      `json:"evidenceLevel"`
	Status               string      `json:"status"`
	Reason               string      `json:"reason,omitempty"`
	SelectedDeviceKey    string      `json:"selectedDeviceKey,omitempty"`
	Assignment           *Assignment `json:"assignment,omitempty"`
	SerializedAssignment string      `json:"serializedAssignment,omitempty"`
	Details              []string    `json:"details,omitempty"`
}

// CanonicalDeviceKey binds a provider DeviceID to the NodeUID that owns it.
// Device IDs are normalized case-insensitively because NVIDIA UUID spelling is
// not a safe cross-component identity boundary by itself.
func CanonicalDeviceKey(nodeUID, deviceID string) (string, error) {
	nodeUID = strings.TrimSpace(nodeUID)
	deviceID = normalizeDeviceID(deviceID)
	if nodeUID == "" {
		return "", fmt.Errorf("node UID is empty")
	}
	if deviceID == "" {
		return "", fmt.Errorf("device ID is empty")
	}
	if strings.ContainsAny(nodeUID, "/\n\r") {
		return "", fmt.Errorf("node UID contains a key separator")
	}
	if strings.ContainsAny(deviceID, "/\n\r") {
		return "", fmt.Errorf("device ID contains a key separator")
	}
	return nodeUID + "/" + deviceID, nil
}

func normalizeDeviceID(deviceID string) string {
	return strings.ToUpper(strings.TrimSpace(deviceID))
}

func canonicalizeAssignment(assignment Assignment) (Assignment, error) {
	if assignment.Version != 1 {
		return Assignment{}, fmt.Errorf("unsupported assignment version %d", assignment.Version)
	}
	if assignment.ResourceName != expectedNVIDIAContract.ResourceName {
		return Assignment{}, fmt.Errorf("unexpected resourceName %q", assignment.ResourceName)
	}
	if assignment.Provider != expectedNVIDIAContract.ProviderID {
		return Assignment{}, fmt.Errorf("unexpected provider %q", assignment.Provider)
	}
	if len(assignment.DeviceKeys) == 0 {
		return Assignment{}, fmt.Errorf("deviceKeys is empty")
	}

	keys := make([]string, 0, len(assignment.DeviceKeys))
	seen := make(map[string]struct{}, len(assignment.DeviceKeys))
	for _, key := range assignment.DeviceKeys {
		key = strings.TrimSpace(key)
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			return Assignment{}, fmt.Errorf("invalid device key %q", key)
		}
		canonicalKey, err := CanonicalDeviceKey(parts[0], parts[1])
		if err != nil {
			return Assignment{}, fmt.Errorf("invalid device key %q: %w", key, err)
		}
		if _, exists := seen[canonicalKey]; exists {
			return Assignment{}, fmt.Errorf("duplicate device key %q", canonicalKey)
		}
		seen[canonicalKey] = struct{}{}
		keys = append(keys, canonicalKey)
	}
	sort.Strings(keys)
	assignment.DeviceKeys = keys
	return assignment, nil
}

// ParseAssignment strictly parses and canonicalizes the minimum assignment
// payload. Unknown fields are rejected so a later producer cannot silently
// create an unreviewed contract extension.
func ParseAssignment(data []byte) (Assignment, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var assignment Assignment
	if err := decoder.Decode(&assignment); err != nil {
		return Assignment{}, fmt.Errorf("decode assignment: %w", err)
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Assignment{}, fmt.Errorf("assignment contains more than one JSON value")
		}
		return Assignment{}, fmt.Errorf("decode trailing assignment data: %w", err)
	}
	return canonicalizeAssignment(assignment)
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
	result := ProbeResult{CaseID: caseID, EvidenceLevel: level}

	if err := validateContract(input.Contract); err != nil {
		return failResult(result, ReasonIdentityMismatch, err.Error())
	}
	if err := validateInventory(input.Devices); err != nil {
		return failResult(result, ReasonAssignmentInvalid, err.Error())
	}
	if !input.ProviderCanConfirmSelected {
		return failResult(result, ReasonAssignmentNotEnforceable,
			"provider did not confirm the scheduler-selected DeviceID")
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
	assignment, err := canonicalizeAssignment(Assignment{
		Version:      1,
		ResourceName: expectedNVIDIAContract.ResourceName,
		Provider:     expectedNVIDIAContract.ProviderID,
		DeviceKeys:   []string{key},
	})
	if err != nil {
		return failResult(result, ReasonAssignmentInvalid, err.Error())
	}
	serialized, err := json.Marshal(assignment)
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
