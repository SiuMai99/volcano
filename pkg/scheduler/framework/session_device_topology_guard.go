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
	"fmt"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

// ValidateXPUTopologyBeforeBind is the M2 final safety guard shared by direct
// and Statement allocation paths. It is intentionally narrow: soft policies
// remain advisory after a valid catalog is loaded, while every hard policy is
// rejected because M2 has neither a scheduler-selected DeviceKey nor an
// assignment persistence/enforcement contract.
//
// Plugin JobValidFn is responsible for richer compiler reasons. This guard
// remains in framework so a missing callback or a future action path cannot
// accidentally enqueue a Binding for a hard policy.
func (ssn *Session) ValidateXPUTopologyBeforeBind(job *api.JobInfo) error {
	if job == nil || !job.HasDeviceTopologyPolicy() {
		return nil
	}

	manager := ssn.XPUTopologyManager()
	if manager == nil {
		return fmt.Errorf("%s: xPU topology manager is unavailable", topology.FeatureDisabled)
	}
	activation := manager.Activation()
	if reason := activation.Reason(); reason != topology.ActivationReady {
		// A missing Provider observation is normal for a soft policy: it loses
		// preference but ordinary scheduling remains available. All other
		// activation failures must fail closed for a non-empty policy.
		if !(reason == topology.ProviderNotReady && !job.HasHardDeviceTopologyPolicy()) {
			return fmt.Errorf("%s: xPU topology policy cannot enter Bind", reason)
		}
	}
	if manager.Catalog() == nil {
		return fmt.Errorf("%s: xPU topology catalog is not ready", topology.CatalogNotReady)
	}

	if job.HasHardDeviceTopologyPolicy() {
		return fmt.Errorf("%s: Advisory M2 does not bind hard xPU topology policies", topology.AssignmentNotEnforceable)
	}
	return nil
}
