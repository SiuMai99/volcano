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

	"volcano.sh/volcano/pkg/scheduler/api"
	assignmentprovider "volcano.sh/volcano/pkg/scheduler/topology/assignment"
)

func TestRunBridgeConfirmsAndPassesThroughSchedulerSelectedKey(t *testing.T) {
	result := RunBridge(testInput())
	if result.Status != StatusPass {
		t.Fatalf("expected bridge pass, got %#v", result)
	}
	if result.Capabilities.ExactReady || result.Capabilities.ConsumeSelectedKey || result.Capabilities.ConfirmKubeletDeviceID {
		t.Fatalf("mock validation bridge overstated exact capabilities: %#v", result.Capabilities)
	}
	if result.SelectedDeviceKey != "node-uid-a/GPU-BBBBBBBB" {
		t.Fatalf("unexpected selected key %q", result.SelectedDeviceKey)
	}
	if result.AssignmentAnnotationKey != api.XPUAssignmentAnnotationKey {
		t.Fatalf("unexpected annotation key %q", result.AssignmentAnnotationKey)
	}
	assignment, err := ParseAssignment([]byte(result.AssignmentAnnotationValue))
	if err != nil {
		t.Fatalf("parse annotation value: %v", err)
	}
	if len(assignment.Assignments) != 1 || len(assignment.Assignments[0].DeviceKeys) != 1 || assignment.Assignments[0].DeviceKeys[0] != result.SelectedDeviceKey {
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
		assignmentRequestForInput(input),
		[]string{key},
	)
	bridgeErr, ok := err.(*assignmentprovider.Error)
	if !ok || bridgeErr.Reason != assignmentprovider.AssignmentNotEnforceable {
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
		assignmentRequestForInput(input),
		[]string{key},
	)
	bridgeErr, ok := err.(*assignmentprovider.Error)
	if !ok || bridgeErr.Reason != assignmentprovider.IdentityMismatch {
		t.Fatalf("expected NodeUID mismatch, got %T %v", err, err)
	}
}

func assignmentRequestForInput(input ProbeInput) AssignmentRequest {
	return AssignmentRequest{
		PodUID:           input.PodUID,
		Container:        input.Container,
		NodeName:         input.NodeName,
		NodeUID:          input.NodeUID,
		SourceGeneration: input.SourceGeneration,
		Contract:         input.Contract,
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
