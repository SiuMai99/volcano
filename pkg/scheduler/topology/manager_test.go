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

package topology

import (
	"errors"
	"testing"
)

func TestActivationMatrix(t *testing.T) {
	tests := []struct {
		name         string
		gate         bool
		plugin       bool
		wantReason   ActivationReason
		wantCanStart bool
	}{
		{name: "gate off plugin absent", gate: false, plugin: false, wantReason: FeatureDisabled, wantCanStart: false},
		{name: "gate off plugin present", gate: false, plugin: true, wantReason: FeatureDisabled, wantCanStart: false},
		{name: "gate on plugin absent", gate: true, plugin: false, wantReason: PluginDisabled, wantCanStart: false},
		{name: "gate on plugin present", gate: true, plugin: true, wantReason: CatalogNotReady, wantCanStart: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			err := manager.Configure(ActivationConfig{
				GateEnabled:           tt.gate,
				PluginConfigured:      tt.plugin,
				PluginConfigReady:     tt.plugin,
				ConfigurationIdentity: tt.name,
			})
			if err != nil {
				t.Fatalf("Configure() error = %v", err)
			}
			activation := manager.Activation()
			if got := activation.Reason(); got != tt.wantReason {
				t.Fatalf("Reason() = %q, want %q", got, tt.wantReason)
			}
			if got := activation.CanStartManager(); got != tt.wantCanStart {
				t.Fatalf("CanStartManager() = %v, want %v", got, tt.wantCanStart)
			}
		})
	}
}

func TestManagerLifecycleStartsOnce(t *testing.T) {
	manager := NewManager()
	if err := manager.Start(); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	if err := manager.Start(); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	if got := manager.StartCount(); got != 1 {
		t.Fatalf("StartCount() = %d, want 1", got)
	}
	if !manager.Started() {
		t.Fatal("manager should be started")
	}

	manager.Stop()
	manager.Stop()
	if manager.Started() {
		t.Fatal("manager should be stopped")
	}
	if err := manager.Start(); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("Start() after Stop() error = %v, want %v", err, ErrManagerStopped)
	}
}

func TestManagerReadinessAndHardPolicy(t *testing.T) {
	manager := NewManager()
	if err := manager.Configure(ActivationConfig{
		GateEnabled:           true,
		PluginConfigured:      true,
		PluginConfigReady:     true,
		ConfigurationIdentity: "annotation:/catalog.json",
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if got := manager.Activation().PolicyReason(false); got != CatalogNotReady {
		t.Fatalf("soft PolicyReason() = %q, want %q", got, CatalogNotReady)
	}

	manager.SetReadiness(Readiness{
		CatalogReady:            true,
		ProviderReady:           true,
		AssignmentContractReady: false,
	})
	activation := manager.Activation()
	if !activation.Ready() {
		t.Fatal("soft activation should be ready")
	}
	if activation.HardPolicyReady() {
		t.Fatal("hard activation must remain unavailable")
	}
	if got := activation.PolicyReason(true); got != AssignmentNotEnforceable {
		t.Fatalf("hard PolicyReason() = %q, want %q", got, AssignmentNotEnforceable)
	}
}

func TestManagerRejectsStaticConfigurationReload(t *testing.T) {
	manager := NewManager()
	first := ActivationConfig{
		GateEnabled:           true,
		PluginConfigured:      true,
		PluginConfigReady:     true,
		ConfigurationIdentity: "annotation:/catalog-a.json",
	}
	if err := manager.Configure(first); err != nil {
		t.Fatalf("first Configure() error = %v", err)
	}
	if err := manager.Configure(first); err != nil {
		t.Fatalf("idempotent Configure() error = %v", err)
	}
	if err := manager.Configure(ActivationConfig{
		GateEnabled:           true,
		PluginConfigured:      true,
		PluginConfigReady:     true,
		ConfigurationIdentity: "annotation:/catalog-b.json",
	}); !errors.Is(err, ErrActivationConfigurationChanged) {
		t.Fatalf("changed Configure() error = %v, want %v", err, ErrActivationConfigurationChanged)
	}
	if got := manager.Activation().Reason(); got != ActivationRestartRequired {
		t.Fatalf("Reason() after reload = %q, want %q", got, ActivationRestartRequired)
	}
}
