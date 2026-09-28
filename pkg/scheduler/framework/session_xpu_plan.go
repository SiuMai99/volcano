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

	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

const xpuTopologyPluginName = "xpu-topology-aware"

// XPUPlannedResource is one tentative scheduler-selected resource. The
// assignment writer and exact bridge are deliberately outside this callback.
type XPUPlannedResource struct {
	Request    api.XPUResourceRequest
	DeviceKeys []api.DeviceKey
	Placements []XPUPlannedPolicyPlacement
}

// XPUHardGroupRef distinguishes a Job policy from a SubGroup policy.
type XPUHardGroupRef struct {
	Job    api.JobID
	SubJob api.SubJobID
}

// XPUPlannedPolicyPlacement records the concrete domain or fabric selected
// for a policy in the winning plan.
type XPUPlannedPolicyPlacement struct {
	Class       api.DomainClassKey
	Group       *XPUHardGroupRef
	LocalDomain *api.LocalDomainKey
	Fabric      *api.FabricKey
}

// XPUHardAnchor is the Group policy instance selected by the planner.
type XPUHardAnchor struct {
	Group       XPUHardGroupRef
	Class       api.DomainClassKey
	LocalDomain *api.LocalDomainKey
	Fabric      *api.FabricKey
}

type XPUPlannedTask struct {
	TaskID    api.TaskID
	PodUID    types.UID
	NodeName  string
	Resources []XPUPlannedResource
}

// XPUHardPlan is a detached value returned for one complete admission wave.
// It authorizes neither assignment persistence nor Bind.
type XPUHardPlan struct {
	Tasks   []XPUPlannedTask
	Anchors []XPUHardAnchor
}

type XPUHardPlanFn func([]AllocationPlacement) (XPUHardPlan, error)

// AddXPUHardPlanFn registers the Session-local, side-effect-free planner.
// The xPU plugin is the only owner; a duplicate registration is unsafe.
func (ssn *Session) AddXPUHardPlanFn(fn XPUHardPlanFn) error {
	if fn == nil || ssn.xpuHardPlanFn != nil {
		return fmt.Errorf("xPU hard planner must be registered exactly once")
	}
	ssn.xpuHardPlanFn = fn
	return nil
}

func (ssn *Session) HasXPUHardPlanFn() bool {
	return ssn != nil && ssn.xpuHardPlanFn != nil
}

// JobValidForXPUHardTrial still runs every configured Job validator. It
// permits only the xPU plugin's final-path blocker during a non-binding trial.
func (ssn *Session) JobValidForXPUHardTrial(job *api.JobInfo) *api.ValidateResult {
	if job == nil || !job.HasHardDeviceTopologyPolicy() || !ssn.HasXPUHardPlanFn() {
		return ssn.JobValid(job)
	}
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			validate, found := ssn.jobValidFns[plugin.Name]
			if !found {
				continue
			}
			if result := validate(job); result != nil && !result.Pass {
				if plugin.Name == xpuTopologyPluginName && result.Reason == string(topology.AssignmentNotEnforceable) {
					continue
				}
				return result
			}
		}
	}
	return nil
}

// PlanXPUHard calls the registered plugin on a detached operation view.
// Missing plugin is a hard-policy blocker, never an implicit success.
func (ssn *Session) PlanXPUHard(view []AllocationPlacement) (XPUHardPlan, error) {
	if !ssn.HasXPUHardPlanFn() {
		return XPUHardPlan{}, fmt.Errorf("%s: xPU hard planner is unavailable", topology.AssignmentNotEnforceable)
	}
	return ssn.xpuHardPlanFn(append([]AllocationPlacement(nil), view...))
}
