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

package framework

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

func TestM2HardDeviceTopologyCannotReachAllocatePaths(t *testing.T) {
	manager := topology.NewManager()
	if err := manager.Configure(topology.ActivationConfig{
		GateEnabled: true, PluginConfigured: true, PluginConfigReady: true, ConfigurationIdentity: "hard-guard-test",
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	configureGuardTestCatalog(t, manager)
	manager.SetReadiness(topology.Readiness{CatalogReady: true, ProviderReady: true, AssignmentContractReady: true})

	task := &api.TaskInfo{
		UID: "task", Job: "job",
		Pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("task")}},
	}
	job := &api.JobInfo{
		UID: "job",
		DeviceTopology: api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
			ResourceName: "nvidia.com/gpu",
			Mode:         scheduling.HardDeviceTopologyMode,
			DomainClass: api.DomainClassKey{
				ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up",
			},
		}}},
	}
	ssn := &Session{xpuTopologyManager: manager, Jobs: map[api.JobID]*api.JobInfo{job.UID: job}}
	node := &api.NodeInfo{Name: "node-a"}

	if err := ssn.Allocate(task, node); err == nil || !strings.Contains(err.Error(), string(topology.AssignmentNotEnforceable)) {
		t.Fatalf("Session.Allocate() error = %v, want %s", err, topology.AssignmentNotEnforceable)
	}
	if task.Pod.Spec.NodeName != "" {
		t.Fatalf("Session.Allocate mutated Pod nodeName before hard guard: %q", task.Pod.Spec.NodeName)
	}
	// dispatch is deliberately invoked with no cache: reaching AddBindTask
	// would panic, so this proves the final, immediately-before-Bind guard.
	if err := ssn.dispatch(task); err == nil || !strings.Contains(err.Error(), string(topology.AssignmentNotEnforceable)) {
		t.Fatalf("Session.dispatch() error = %v, want %s", err, topology.AssignmentNotEnforceable)
	}
	if err := NewStatement(ssn).Allocate(task, node); err == nil || !strings.Contains(err.Error(), string(topology.AssignmentNotEnforceable)) {
		t.Fatalf("Statement.Allocate() error = %v, want %s", err, topology.AssignmentNotEnforceable)
	}
	// Statement.allocate is Commit's last route to AddBindTask.
	if err := NewStatement(ssn).allocate(task); err == nil || !strings.Contains(err.Error(), string(topology.AssignmentNotEnforceable)) {
		t.Fatalf("Statement.allocate() error = %v, want %s", err, topology.AssignmentNotEnforceable)
	}
	if task.Pod.Spec.NodeName != "" {
		t.Fatalf("Statement.Allocate mutated Pod nodeName before hard guard: %q", task.Pod.Spec.NodeName)
	}
}

func TestM2SoftPolicyMayBindWhenProviderHasNoFacts(t *testing.T) {
	manager := topology.NewManager()
	if err := manager.Configure(topology.ActivationConfig{
		GateEnabled: true, PluginConfigured: true, PluginConfigReady: true, ConfigurationIdentity: "soft-guard-test",
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	configureGuardTestCatalog(t, manager)
	manager.SetReadiness(topology.Readiness{CatalogReady: true, ProviderReady: false})

	job := &api.JobInfo{UID: "job", DeviceTopology: api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
		ResourceName: "nvidia.com/gpu", Mode: scheduling.SoftDeviceTopologyMode,
		DomainClass: api.DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"},
	}}}}
	ssn := &Session{xpuTopologyManager: manager}
	if err := ssn.ValidateXPUTopologyBeforeBind(job); err != nil {
		t.Fatalf("soft policy with ProviderNotReady must remain advisory, got %v", err)
	}
}

func TestM2PolicyCannotBypassMissingManagerOrCatalog(t *testing.T) {
	job := &api.JobInfo{UID: "job", DeviceTopology: api.CanonicalDeviceTopologySpec{Policies: []api.CanonicalDeviceTopologyPolicy{{
		ResourceName: "nvidia.com/gpu", Mode: scheduling.SoftDeviceTopologyMode,
		DomainClass: api.DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"},
	}}}}
	if err := (&Session{}).ValidateXPUTopologyBeforeBind(job); err == nil || !strings.Contains(err.Error(), string(topology.FeatureDisabled)) {
		t.Fatalf("missing manager error = %v, want %s", err, topology.FeatureDisabled)
	}

	manager := topology.NewManager()
	if err := manager.Configure(topology.ActivationConfig{
		GateEnabled: true, PluginConfigured: true, PluginConfigReady: true, CatalogReady: true, ProviderReady: true, ConfigurationIdentity: "missing-catalog",
	}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	ssn := &Session{xpuTopologyManager: manager}
	if err := ssn.ValidateXPUTopologyBeforeBind(job); err == nil || !strings.Contains(err.Error(), string(topology.CatalogNotReady)) {
		t.Fatalf("missing catalog error = %v, want %s", err, topology.CatalogNotReady)
	}
}

func configureGuardTestCatalog(t *testing.T, manager *topology.Manager) {
	t.Helper()
	catalog, err := topology.LoadCatalog(strings.NewReader(`{
  "resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Node","name":"local-scale-up"}]}]
}`))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	if err := manager.ConfigureCatalog(catalog); err != nil {
		t.Fatalf("ConfigureCatalog() error = %v", err)
	}
}
