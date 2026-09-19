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
	"testing"
)

func testInput() ProbeInput {
	return ProbeInput{
		CaseID:        "nvidia-nvml-mock-selected-uuid-idempotent",
		EvidenceLevel: "L0",
		Contract:      expectedNVIDIAContract,
		NodeName:      "volcano-gpu-mvp-worker",
		NodeUID:       "node-uid-a",
		Devices: []MockDevice{
			{NodeName: "volcano-gpu-mvp-worker", NodeUID: "node-uid-a", DeviceID: "GPU-aaaaaaaa", Healthy: true},
			{NodeName: "volcano-gpu-mvp-worker", NodeUID: "node-uid-a", DeviceID: "GPU-bbbbbbbb", Healthy: true},
			{NodeName: "volcano-gpu-mvp-worker2", NodeUID: "node-uid-b", DeviceID: "GPU-aaaaaaaa", Healthy: true},
		},
		SelectedDeviceID:           "gpu-bbbbbbbb",
		ProviderCanConfirmSelected: true,
	}
}

func TestRunProbeSelectedUUID(t *testing.T) {
	result := RunProbe(testInput())
	if result.Status != StatusPass {
		t.Fatalf("expected pass, got %#v", result)
	}
	if result.SelectedDeviceKey != "node-uid-a/GPU-BBBBBBBB" {
		t.Fatalf("unexpected selected key %q", result.SelectedDeviceKey)
	}
	if result.Assignment == nil || len(result.Assignment.DeviceKeys) != 1 {
		t.Fatalf("expected one assignment key, got %#v", result.Assignment)
	}
	parsed, err := ParseAssignment([]byte(result.SerializedAssignment))
	if err != nil {
		t.Fatalf("parse serialized assignment: %v", err)
	}
	if got := parsed.DeviceKeys[0]; got != result.SelectedDeviceKey {
		t.Fatalf("round-trip key=%q, want %q", got, result.SelectedDeviceKey)
	}
}

func TestRunProbeFailsWhenProviderCannotConfirmSelectedKey(t *testing.T) {
	input := testInput()
	input.ProviderCanConfirmSelected = false
	result := RunProbe(input)
	if result.Status != StatusFail || result.Reason != ReasonAssignmentNotEnforceable {
		t.Fatalf("expected not-enforceable failure, got %#v", result)
	}
}

func TestRunProbeRejectsNodeUIDMismatch(t *testing.T) {
	input := testInput()
	input.NodeUID = "node-uid-replaced"
	result := RunProbe(input)
	if result.Status != StatusFail || result.Reason != ReasonIdentityMismatch {
		t.Fatalf("expected identity mismatch, got %#v", result)
	}
}

func TestRunProbeRejectsUnhealthySelectedDevice(t *testing.T) {
	input := testInput()
	input.Devices[1].Healthy = false
	result := RunProbe(input)
	if result.Status != StatusFail || result.Reason != ReasonTopologyNotReady {
		t.Fatalf("expected topology-not-ready failure, got %#v", result)
	}
}

func TestInventoryAllowsCrossNodeSameDeviceIDButRejectsSameNodeCollision(t *testing.T) {
	input := testInput()
	if err := validateInventory(input.Devices); err != nil {
		t.Fatalf("same device ID on different nodes should be valid: %v", err)
	}
	input.Devices = append(input.Devices, input.Devices[0])
	if err := validateInventory(input.Devices); err == nil {
		t.Fatal("same NodeUID and DeviceID should be rejected")
	}
}

func TestAssignmentCanonicalizationIsSortedAndIdempotent(t *testing.T) {
	assignment := Assignment{
		Version:      1,
		ResourceName: expectedNVIDIAContract.ResourceName,
		Provider:     expectedNVIDIAContract.ProviderID,
		DeviceKeys:   []string{"node-b/gpu-b", "node-a/GPU-A"},
	}
	canonical, err := canonicalizeAssignment(assignment)
	if err != nil {
		t.Fatalf("canonicalize assignment: %v", err)
	}
	if got, want := canonical.DeviceKeys[0], "node-a/GPU-A"; got != want {
		t.Fatalf("first key=%q, want %q", got, want)
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		t.Fatalf("marshal assignment: %v", err)
	}
	parsed, err := ParseAssignment(data)
	if err != nil {
		t.Fatalf("parse assignment: %v", err)
	}
	if got, want := parsed.DeviceKeys[1], "node-b/GPU-B"; got != want {
		t.Fatalf("second key=%q, want %q", got, want)
	}
}

func TestParseAssignmentRejectsUnknownFieldsAndDuplicateKeys(t *testing.T) {
	unknownField := []byte(`{"version":1,"resourceName":"nvidia.com/gpu","provider":"nvidia-nvml-v1","deviceKeys":["node/GPU-A"],"planDigest":"not-contract"}`)
	if _, err := ParseAssignment(unknownField); err == nil {
		t.Fatal("unknown assignment fields should be rejected")
	}
	duplicateKeys := []byte(`{"version":1,"resourceName":"nvidia.com/gpu","provider":"nvidia-nvml-v1","deviceKeys":["node/GPU-A","node/gpu-a"]}`)
	if _, err := ParseAssignment(duplicateKeys); err == nil {
		t.Fatal("duplicate canonical assignment keys should be rejected")
	}
}
