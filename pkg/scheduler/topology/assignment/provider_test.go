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

package assignment

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology/provider"
)

func TestCapabilitiesDoNotPromoteValidationOnlyProvider(t *testing.T) {
	validationOnly := Capabilities{ValidateSelectedDeviceKeys: true}
	if validationOnly.ExactReady() {
		t.Fatal("validation-only mock Provider must not be exact ready")
	}
	exact := Capabilities{
		ValidateSelectedDeviceKeys: true,
		ConsumeSelectedDeviceKeys:  true,
		ConfirmKubeletDeviceIDs:    true,
	}
	if !exact.ExactReady() {
		t.Fatal("complete L1 capabilities should be exact ready")
	}
	invalid := Capabilities{ConfirmKubeletDeviceIDs: true}
	if err := invalid.Validate(); err == nil {
		t.Fatal("kubelet confirmation without consumption should be invalid")
	}
}

func TestStaticInventoryProviderPassesThroughSelectedKeys(t *testing.T) {
	identity := provider.ProviderIdentityRef{ProviderID: "nvidia-nvml-v1", Namespace: "nvidia.com"}
	request, devices := testSelectedAssignment(identity)
	validator := &StaticInventoryProvider{
		ProviderIdentity: identity,
		ProfileName:      "mock-nvidia-nvml-validation",
		CapabilitySet:    Capabilities{ValidateSelectedDeviceKeys: true},
		Devices:          devices,
	}
	confirmation, err := validator.ValidateSelected(context.Background(), request)
	if err != nil {
		t.Fatalf("ValidateSelected() error = %v", err)
	}
	if confirmation.Profile != validator.ProfileName {
		t.Fatalf("profile = %q, want %q", confirmation.Profile, validator.ProfileName)
	}
	if len(confirmation.AcceptedDeviceKeys) != len(request.DeviceKeys) || confirmation.AcceptedDeviceKeys[0] != request.DeviceKeys[0] {
		t.Fatalf("accepted keys = %#v, want unchanged %#v", confirmation.AcceptedDeviceKeys, request.DeviceKeys)
	}
	if validator.Capabilities().ExactReady() {
		t.Fatal("static validation fixture must not become exact ready")
	}
}

func TestStaticInventoryProviderFailsClosed(t *testing.T) {
	identity := provider.ProviderIdentityRef{ProviderID: "nvidia-nvml-v1", Namespace: "nvidia.com"}
	request, devices := testSelectedAssignment(identity)
	newValidator := func() *StaticInventoryProvider {
		inventory := make(map[api.DeviceKey]api.TopologyDevice, len(devices))
		for key, device := range devices {
			inventory[key] = device
		}
		return &StaticInventoryProvider{
			ProviderIdentity: identity,
			ProfileName:      "mock-nvidia-nvml-validation",
			CapabilitySet:    Capabilities{ValidateSelectedDeviceKeys: true},
			Devices:          inventory,
		}
	}
	tests := []struct {
		name       string
		mutate     func(*StaticInventoryProvider, *SelectedAssignment)
		wantReason Reason
	}{
		{
			name: "unsupported capability",
			mutate: func(p *StaticInventoryProvider, _ *SelectedAssignment) {
				p.CapabilitySet = Capabilities{}
			},
			wantReason: AssignmentNotEnforceable,
		},
		{
			name: "NodeUID replacement",
			mutate: func(_ *StaticInventoryProvider, request *SelectedAssignment) {
				request.Node.UID = "replacement"
			},
			wantReason: IdentityMismatch,
		},
		{
			name: "unhealthy device",
			mutate: func(p *StaticInventoryProvider, request *SelectedAssignment) {
				device := p.Devices[request.DeviceKeys[0]]
				device.Health = api.DeviceUnhealthy
				p.Devices[request.DeviceKeys[0]] = device
			},
			wantReason: TopologyNotReady,
		},
		{
			name: "provider mismatch",
			mutate: func(_ *StaticInventoryProvider, request *SelectedAssignment) {
				request.DeviceKeys[0].ID.ProviderID = "other"
			},
			wantReason: IdentityMismatch,
		},
		{
			name: "source generation changed",
			mutate: func(_ *StaticInventoryProvider, request *SelectedAssignment) {
				request.SourceGeneration++
			},
			wantReason: TopologyNotReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := newValidator()
			candidate := request
			candidate.DeviceKeys = append([]api.DeviceKey(nil), request.DeviceKeys...)
			tt.mutate(validator, &candidate)
			_, err := validator.ValidateSelected(context.Background(), candidate)
			assignmentErr, ok := err.(*Error)
			if !ok || assignmentErr.Reason != tt.wantReason {
				t.Fatalf("ValidateSelected() error = %T %v, want reason %q", err, err, tt.wantReason)
			}
		})
	}
}

func TestValidateSelectedAssignmentRejectsDuplicateKeys(t *testing.T) {
	identity := provider.ProviderIdentityRef{ProviderID: "nvidia-nvml-v1", Namespace: "nvidia.com"}
	request, _ := testSelectedAssignment(identity)
	request.DeviceKeys = append(request.DeviceKeys, request.DeviceKeys[0])
	err := ValidateSelectedAssignment(identity, request)
	assignmentErr, ok := err.(*Error)
	if !ok || assignmentErr.Reason != AssignmentInvalid || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("ValidateSelectedAssignment() error = %T %v", err, err)
	}
}

func testSelectedAssignment(identity provider.ProviderIdentityRef) (SelectedAssignment, map[api.DeviceKey]api.TopologyDevice) {
	key := api.DeviceKey{
		ResourceName: "nvidia.com/gpu",
		OwnerNodeUID: "node-uid-a",
		ID: api.DeviceID{
			ProviderID: identity.ProviderID,
			Namespace:  identity.Namespace,
			Value:      "GPU-AAAAAAAA",
		},
	}
	request := SelectedAssignment{
		PodUID:           types.UID("pod-uid-a"),
		Node:             api.NodeIdentity{Name: "node-a", UID: key.OwnerNodeUID},
		Container:        api.XPUContainerRef{Kind: api.XPUContainerRegular, Name: "worker"},
		ResourceName:     corev1.ResourceName("nvidia.com/gpu"),
		DeviceKeys:       []api.DeviceKey{key},
		SourceGeneration: 1,
	}
	devices := map[api.DeviceKey]api.TopologyDevice{
		key: {Key: key, NodeName: "node-a", Health: api.DeviceHealthy, SourceGeneration: 1},
	}
	return request, devices
}
