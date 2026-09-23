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
	"testing"

	corev1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func testInput() ProbeInput {
	return ProbeInput{
		CaseID:        "nvidia-nvml-mock-selected-uuid-idempotent",
		EvidenceLevel: "L0",
		Contract:      expectedNVIDIAContract,
		PodUID:        "pod-uid-a",
		Container: api.XPUContainerRef{
			Kind: api.XPUContainerRegular,
			Name: "worker",
		},
		NodeName:         "volcano-gpu-mvp-worker",
		NodeUID:          "node-uid-a",
		SourceGeneration: 1,
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
	if result.Capabilities.ExactReady || !result.Capabilities.ValidateSelectedKey {
		t.Fatalf("unexpected L0 capability matrix: %#v", result.Capabilities)
	}
	if result.SelectedDeviceKey != "node-uid-a/GPU-BBBBBBBB" {
		t.Fatalf("unexpected selected key %q", result.SelectedDeviceKey)
	}
	if result.Assignment == nil || len(result.Assignment.Assignments) != 1 || len(result.Assignment.Assignments[0].DeviceKeys) != 1 {
		t.Fatalf("expected one assignment key, got %#v", result.Assignment)
	}
	parsed, err := ParseAssignment([]byte(result.SerializedAssignment))
	if err != nil {
		t.Fatalf("parse serialized assignment: %v", err)
	}
	if got := parsed.Assignments[0].DeviceKeys[0]; got != result.SelectedDeviceKey {
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

func TestRunProbeRejectsIncompleteAssignmentIdentity(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*ProbeInput)
		wantReason string
	}{
		{name: "missing PodUID", mutate: func(input *ProbeInput) { input.PodUID = "" }, wantReason: ReasonAssignmentInvalid},
		{name: "missing ContainerRef", mutate: func(input *ProbeInput) { input.Container = api.XPUContainerRef{} }, wantReason: ReasonAssignmentInvalid},
		{name: "missing generation", mutate: func(input *ProbeInput) { input.SourceGeneration = 0 }, wantReason: ReasonTopologyNotReady},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := testInput()
			tt.mutate(&input)
			result := RunProbe(input)
			if result.Status != StatusFail || result.Reason != tt.wantReason {
				t.Fatalf("RunProbe() = %#v, want reason %s", result, tt.wantReason)
			}
		})
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
	assignment := api.XPUAssignment{
		Version: api.XPUAssignmentVersion,
		Assignments: []api.XPUAssignmentEntry{{
			Container:    api.XPUContainerRef{Kind: api.XPUContainerRegular, Name: "worker"},
			ResourceName: corev1.ResourceName(expectedNVIDIAContract.ResourceName),
			Provider:     expectedNVIDIAContract.ProviderID,
			DeviceKeys:   []string{"node-b/GPU-B", "node-a/GPU-A"},
		}},
	}
	canonical, err := canonicalizeAssignment(assignment)
	if err != nil {
		t.Fatalf("canonicalize assignment: %v", err)
	}
	if got, want := canonical.Assignments[0].DeviceKeys[0], "node-a/GPU-A"; got != want {
		t.Fatalf("first key=%q, want %q", got, want)
	}
	data, err := api.MarshalXPUAssignment(canonical)
	if err != nil {
		t.Fatalf("marshal assignment: %v", err)
	}
	parsed, err := ParseAssignment(data)
	if err != nil {
		t.Fatalf("parse assignment: %v", err)
	}
	if got, want := parsed.Assignments[0].DeviceKeys[1], "node-b/GPU-B"; got != want {
		t.Fatalf("second key=%q, want %q", got, want)
	}
}

func TestParseAssignmentRejectsUnknownFieldsAndDuplicateKeys(t *testing.T) {
	unknownField := []byte(`{"version":1,"assignments":[{"container":{"kind":"regular","name":"worker"},"resourceName":"nvidia.com/gpu","provider":"nvidia-nvml-v1","deviceKeys":["node/GPU-A"],"planDigest":"not-contract"}]}`)
	if _, err := ParseAssignment(unknownField); err == nil {
		t.Fatal("unknown assignment fields should be rejected")
	}
	duplicateKeys := []byte(`{"version":1,"assignments":[{"container":{"kind":"regular","name":"worker"},"resourceName":"nvidia.com/gpu","provider":"nvidia-nvml-v1","deviceKeys":["node/GPU-A","node/GPU-A"]}]}`)
	if _, err := ParseAssignment(duplicateKeys); err == nil {
		t.Fatal("duplicate canonical assignment keys should be rejected")
	}
}
