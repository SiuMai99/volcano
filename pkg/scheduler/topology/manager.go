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

// Package topology contains process-scoped xPU topology lifecycle primitives.
//
// This package deliberately does not discover devices, read a catalog, or
// register scheduler callbacks. Those concerns are added by later XPU PRs.
// The manager here only owns activation state and gives the scheduler/cache a
// single, injectable lifecycle object instead of a package-global singleton.
package topology

import (
	"errors"
	"sync"
)

// ActivationReason is the stable, low-cardinality reason for an activation
// decision. The values are also suitable for status/events and metrics labels.
type ActivationReason string

const (
	ActivationReady           ActivationReason = "XPUTopologyResolved"
	FeatureDisabled           ActivationReason = "XPUTopologyFeatureDisabled"
	PluginDisabled            ActivationReason = "XPUTopologyPluginDisabled"
	PluginConfigInvalid       ActivationReason = "XPUTopologyPluginConfigInvalid"
	ActivationRestartRequired ActivationReason = "XPUTopologyActivationRestartRequired"
	CatalogNotReady           ActivationReason = "XPUTopologyCatalogNotReady"
	ProviderNotReady          ActivationReason = "XPUTopologyProviderNotReady"
	AssignmentNotEnforceable  ActivationReason = "XPUAssignmentNotEnforceable"
)

var (
	// ErrActivationConfigurationChanged is returned when a process-scoped
	// activation identity changes after it has been configured. A scheduler
	// config reload must not silently replace the identity of a running xPU
	// manager.
	ErrActivationConfigurationChanged = errors.New("xpu topology activation configuration changed; restart required")
	// ErrManagerStopped prevents a stopped process manager from being started
	// again as if it were a new process.
	ErrManagerStopped = errors.New("xpu topology manager has been stopped")
	// ErrCatalogConfigurationChanged prevents a running scheduler from
	// replacing its fixed Alpha catalog. A changed catalog requires a process
	// restart, not a scheduler-config reload.
	ErrCatalogConfigurationChanged = errors.New("xpu topology catalog has already been configured; restart required")
)

// ActivationConfig contains static process configuration. Runtime readiness
// is included so tests and future Provider/catalog implementations can publish
// an initial complete state atomically; normal scheduler startup leaves those
// fields false until the corresponding PR implements them.
type ActivationConfig struct {
	GateEnabled             bool
	PluginConfigured        bool
	PluginConfigReady       bool
	CatalogReady            bool
	ProviderReady           bool
	AssignmentContractReady bool

	// ConfigurationIdentity is a canonical identity for the static plugin
	// arguments. It is compared on reload, but never exposed as a high
	// cardinality workload label.
	ConfigurationIdentity string
}

// Readiness is the runtime portion of activation state. It is intentionally
// separate from gate/plugin configuration because a valid process activation
// may still be waiting for a catalog or Provider observation.
type Readiness struct {
	CatalogReady            bool
	ProviderReady           bool
	AssignmentContractReady bool
}

// Activation is the immutable-by-copy view of the manager's current
// activation decision.
type Activation struct {
	GateEnabled             bool
	PluginConfigured        bool
	PluginConfigReady       bool
	CatalogReady            bool
	ProviderReady           bool
	AssignmentContractReady bool
	RestartRequired         bool
}

// Ready reports whether the soft topology path may consume the manager. It
// intentionally does not imply that a hard policy can be enforced.
func (a Activation) Ready() bool {
	return a.GateEnabled && a.PluginConfigured && a.PluginConfigReady &&
		a.CatalogReady && a.ProviderReady && !a.RestartRequired
}

// HardPolicyReady reports whether the later exact-assignment contract is also
// available. The activation skeleton never sets this bit by itself.
func (a Activation) HardPolicyReady() bool {
	return a.Ready() && a.AssignmentContractReady
}

// CanStartManager reports whether process-scoped work is allowed to start.
// With either half of the double opt-in missing, no Provider/cache worker is
// started.
func (a Activation) CanStartManager() bool {
	return a.GateEnabled && a.PluginConfigured && a.PluginConfigReady && !a.RestartRequired
}

// Reason returns the reason for the soft activation path.
func (a Activation) Reason() ActivationReason {
	switch {
	case !a.GateEnabled:
		return FeatureDisabled
	case !a.PluginConfigured:
		return PluginDisabled
	case !a.PluginConfigReady:
		return PluginConfigInvalid
	case a.RestartRequired:
		return ActivationRestartRequired
	case !a.CatalogReady:
		return CatalogNotReady
	case !a.ProviderReady:
		return ProviderNotReady
	default:
		return ActivationReady
	}
}

// PolicyReason returns the reason a policy must receive. Hard policy is
// deliberately stricter than the soft activation path until a later PR
// supplies selected DeviceKey confirmation and assignment persistence.
func (a Activation) PolicyReason(hard bool) ActivationReason {
	if reason := a.Reason(); reason != ActivationReady {
		return reason
	}
	if hard && !a.AssignmentContractReady {
		return AssignmentNotEnforceable
	}
	return ActivationReady
}

// ProcessManager is the minimal connection surface owned by SchedulerCache.
// Implementations must be process-scoped; Session plugins may observe the
// object but must not start or stop it.
type ProcessManager interface {
	Start() error
	Stop()
	Started() bool
	Configure(ActivationConfig) error
	SetReadiness(Readiness)
	Activation() Activation
	ConfigureCatalog(*Catalog) error
	CatalogConfigured() bool
	Catalog() *Catalog
}

// Manager owns one process-scoped lifecycle and activation state.
type Manager struct {
	mu sync.RWMutex

	started    bool
	stopped    bool
	startCount int
	configured bool
	identity   string
	activation Activation

	catalogConfigured bool
	catalog           *Catalog
}

var _ ProcessManager = (*Manager)(nil)

// NewManager constructs an inactive manager. Construction has no live
// Provider, catalog, goroutine, or network side effect.
func NewManager() *Manager {
	return &Manager{}
}

// Configure installs static activation state once. Reapplying the same
// identity is idempotent; changing it marks the manager restart-required and
// fails closed instead of hot-swapping process identity.
func (m *Manager) Configure(config ActivationConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.configured {
		m.configured = true
		m.identity = config.ConfigurationIdentity
		m.activation = Activation{
			GateEnabled:             config.GateEnabled,
			PluginConfigured:        config.PluginConfigured,
			PluginConfigReady:       config.PluginConfigReady,
			CatalogReady:            config.CatalogReady,
			ProviderReady:           config.ProviderReady,
			AssignmentContractReady: config.AssignmentContractReady,
		}
		return nil
	}

	if m.identity == config.ConfigurationIdentity &&
		m.activation.GateEnabled == config.GateEnabled &&
		m.activation.PluginConfigured == config.PluginConfigured &&
		m.activation.PluginConfigReady == config.PluginConfigReady {
		return nil
	}

	m.activation.GateEnabled = config.GateEnabled
	m.activation.PluginConfigured = config.PluginConfigured
	m.activation.PluginConfigReady = config.PluginConfigReady
	m.activation.CatalogReady = false
	m.activation.ProviderReady = false
	m.activation.AssignmentContractReady = false
	m.activation.RestartRequired = true
	return ErrActivationConfigurationChanged
}

// SetReadiness publishes runtime readiness without replacing static process
// identity. It is safe to call before Start and from future Provider/catalog
// publishers.
func (m *Manager) SetReadiness(readiness Readiness) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Once a static catalog has been configured, a runtime readiness publisher
	// cannot replace its authoritative load result with a boolean. This keeps a
	// missing/invalid catalog fail-closed and a valid catalog fixed until restart.
	if m.catalogConfigured {
		m.activation.CatalogReady = m.catalog != nil
	} else {
		m.activation.CatalogReady = readiness.CatalogReady
	}
	m.activation.ProviderReady = readiness.ProviderReady
	m.activation.AssignmentContractReady = readiness.AssignmentContractReady
}

// ConfigureCatalog installs the fixed catalog once for this process manager.
// A nil value records a failed/missing initial load and remains fail-closed;
// a later file appearance must be handled by restarting the scheduler rather
// than silently changing catalog authority during a process lifetime.
func (m *Manager) ConfigureCatalog(catalog *Catalog) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.catalogConfigured {
		return ErrCatalogConfigurationChanged
	}
	m.catalogConfigured = true
	m.catalog = catalog
	m.activation.CatalogReady = catalog != nil
	return nil
}

// CatalogConfigured reports whether the static catalog load was attempted.
// Scheduler config reloads use it to avoid re-reading a catalog file.
func (m *Manager) CatalogConfigured() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.catalogConfigured
}

// Catalog returns the fixed immutable catalog selected during initial process
// activation. Catalog itself returns copies of any descriptor data it exposes.
func (m *Manager) Catalog() *Catalog {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.catalog
}

// Start starts the process lifecycle at most once.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return ErrManagerStopped
	}
	if m.started {
		return nil
	}
	m.started = true
	m.startCount++
	return nil
}

// Stop is idempotent and terminal for the process manager.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	m.started = false
	m.stopped = true
}

// Started reports whether the manager is currently started.
func (m *Manager) Started() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.started
}

// StartCount is intentionally observable for lifecycle tests. It is not a
// scheduling metric and must not be used as runtime state by callers.
func (m *Manager) StartCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.startCount
}

// Activation returns a copy, so callers cannot mutate published manager state.
func (m *Manager) Activation() Activation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activation
}
