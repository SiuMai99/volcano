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
)

func TestRunBridgeConfirmsAndPassesThroughSchedulerSelectedKey(t *testing.T) {
	result := RunBridge(testInput())
	if result.Status != StatusPass {
		t.Fatalf("expected bridge pass, got %#v", result)
	}
	if result.SelectedDeviceKey != "node-uid-a/GPU-BBBBBBBB" {
		t.Fatalf("unexpected selected key %q", result.SelectedDeviceKey)
	}
	if result.AssignmentAnnotationKey != AssignmentAnnotationKey {
		t.Fatalf("unexpected annotation key %q", result.AssignmentAnnotationKey)
	}
	assignment, err := ParseAssignment([]byte(result.AssignmentAnnotationValue))
	if err != nil {
		t.Fatalf("parse annotation value: %v", err)
	}
	if len(assignment.DeviceKeys) != 1 || assignment.DeviceKeys[0] != result.SelectedDeviceKey {
		t.Fatalf("bridge changed selected key: assignment=%#v result=%q", assignment, result.SelectedDeviceKey)
	}
}

func TestAssignmentAdapterRejectsStockProvider(t *testing.T) {
	input := testInput()
	key, err := CanonicalDeviceKey(input.NodeUID, input.SelectedDeviceID)
	if err != nil {
		t.Fatalf("canonical selected key: %v", err)
	}
	_, _, err = (AssignmentAdapter{Provider: StockNVIDIAProvider{}}).BuildAssignment(
		AssignmentRequest{NodeName: input.NodeName, NodeUID: input.NodeUID, Contract: input.Contract},
		[]string{key},
	)
	bridgeErr, ok := err.(*BridgeError)
	if !ok || bridgeErr.Reason != ReasonAssignmentNotEnforceable {
		t.Fatalf("expected stock-provider gap, got %T %v", err, err)
	}
}

func TestMockProviderRejectsSelectedKeyFromAnotherNode(t *testing.T) {
	input := testInput()
	key, err := CanonicalDeviceKey("node-uid-b", "GPU-aaaaaaaa")
	if err != nil {
		t.Fatalf("canonical selected key: %v", err)
	}
	_, _, err = (AssignmentAdapter{Provider: NewMockNVIDIAProvider(input.Devices)}).BuildAssignment(
		AssignmentRequest{NodeName: input.NodeName, NodeUID: input.NodeUID, Contract: input.Contract},
		[]string{key},
	)
	bridgeErr, ok := err.(*BridgeError)
	if !ok || bridgeErr.Reason != ReasonIdentityMismatch {
		t.Fatalf("expected NodeUID mismatch, got %T %v", err, err)
	}
}

func TestMockProviderRejectsUnhealthySelectedKey(t *testing.T) {
	input := testInput()
	input.Devices[1].Healthy = false
	result := RunBridge(input)
	if result.Status != StatusFail || result.Reason != ReasonTopologyNotReady {
		t.Fatalf("expected unhealthy-device failure, got %#v", result)
	}
}
