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

package xputopologyaware

import (
	"fmt"
	"time"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/topology"
)

// sessionHardPlanner adapts the Statement's read-only admission view to PR10's
// fixed-Node planner. It keeps no trial result or selected DeviceKey state.
type sessionHardPlanner struct {
	ssn      *framework.Session
	compiler *compiler
	now      func() time.Time
}

func (p *sessionHardPlanner) plan(view []framework.AllocationPlacement) (framework.XPUHardPlan, error) {
	if p == nil || p.ssn == nil || p.compiler == nil {
		return framework.XPUHardPlan{}, fmt.Errorf("%s: xPU hard planner is unavailable", topology.AssignmentNotEnforceable)
	}
	if p.ssn.DeviceTopology != p.compiler.snapshot {
		return framework.XPUHardPlan{}, fmt.Errorf("%s: Session topology view changed after policy compilation", api.XPUTopologyDataNotReadyReason)
	}
	now := p.now
	if now == nil {
		now = time.Now
	}
	input := HardPlanInput{Snapshot: p.ssn.DeviceTopology, Now: now()}
	seen := make(map[api.TaskID]struct{}, len(view))
	for _, placement := range view {
		if _, exists := seen[placement.TaskID]; exists {
			return framework.XPUHardPlan{}, fmt.Errorf("%s: duplicate Task %s in admission wave", api.XPUTopologyPolicyInvalidReason, placement.TaskID)
		}
		seen[placement.TaskID] = struct{}{}
		job := p.ssn.Jobs[placement.JobID]
		if job == nil || !job.HasHardDeviceTopologyPolicy() {
			return framework.XPUHardPlan{}, fmt.Errorf("%s: Task %s has no hard-policy Job", api.XPUTopologyPolicyInvalidReason, placement.TaskID)
		}
		task := job.Tasks[placement.TaskID]
		if task == nil || task.Pod == nil || task.Pod.UID != placement.PodUID || task.NodeName != placement.NodeName {
			return framework.XPUHardPlan{}, fmt.Errorf("%s: Task %s changed after Statement allocation", api.XPUTopologyDataNotReadyReason, placement.TaskID)
		}
		node := p.ssn.Nodes[placement.NodeName]
		if node == nil || node.Node == nil || input.Snapshot == nil ||
			input.Snapshot.Nodes[placement.NodeName].Identity.UID != node.Node.UID || node.Node.UID == "" {
			return framework.XPUHardPlan{}, fmt.Errorf("%s: Node identity for Task %s is not paired with topology", api.XPUTopologyDataNotReadyReason, placement.TaskID)
		}
		compiled := p.compiler.compileTask(job, task)
		if compiled.blocked != nil && compiled.blocked.Reason != string(topology.AssignmentNotEnforceable) {
			return framework.XPUHardPlan{}, fmt.Errorf("%s: %s", compiled.blocked.Reason, compiled.blocked.Message)
		}
		if len(compiled.hardPolicies) == 0 {
			continue
		}
		input.Tasks = append(input.Tasks, HardPlanTask{
			Task: task, NodeName: placement.NodeName, Policies: compiled.hardPolicies,
		})
	}
	if len(input.Tasks) == 0 {
		return framework.XPUHardPlan{}, nil
	}
	// Pod-derived anchors and bound DeviceKey occupancy are supplied by PR12.
	// Until then, an existing bound member cannot be treated as an empty anchor.
	checkedJobs := make(map[api.JobID]struct{}, len(input.Tasks))
	for _, item := range input.Tasks {
		job := p.ssn.Jobs[item.Task.Job]
		if _, checked := checkedJobs[job.UID]; checked {
			continue
		}
		checkedJobs[job.UID] = struct{}{}
		for _, member := range job.Tasks {
			// Allocated can be another SubGroup's speculative Statement in
			// this same Job trial. Only an already dispatched/bound member
			// requires the recovery contract that PR12 supplies.
			if _, current := seen[member.UID]; !current && member.NodeName != "" &&
				(member.Status == api.Binding || member.Status == api.Bound || member.Status == api.Running) &&
				len(p.compiler.compileTask(job, member).hardPolicies) != 0 {
				return framework.XPUHardPlan{}, fmt.Errorf("%s: bound member %s requires Pod-derived anchor recovery", api.XPUTopologyDataNotReadyReason, member.UID)
			}
		}
	}
	plan, failure := PlanHardGroup(input)
	if failure != nil {
		return framework.XPUHardPlan{}, fmt.Errorf("%s: %s", failure.Reason, failure.Detail)
	}
	result := framework.XPUHardPlan{Tasks: make([]framework.XPUPlannedTask, 0, len(plan.Tasks))}
	for _, item := range plan.Tasks {
		planned := framework.XPUPlannedTask{TaskID: item.TaskID, PodUID: item.PodUID, NodeName: item.NodeName}
		for _, resource := range item.Resources {
			plannedResource := framework.XPUPlannedResource{
				Request: resource.Request, DeviceKeys: append([]api.DeviceKey(nil), resource.DeviceKeys...),
			}
			for _, placement := range resource.Placements {
				converted := framework.XPUPlannedPolicyPlacement{
					Class: placement.Class, LocalDomain: copyLocal(placement.LocalDomain), Fabric: copyFabric(placement.Fabric),
				}
				if placement.Group != nil {
					converted.Group = &framework.XPUHardGroupRef{Job: placement.Group.Job, SubJob: placement.Group.SubJob}
				}
				plannedResource.Placements = append(plannedResource.Placements, converted)
			}
			planned.Resources = append(planned.Resources, plannedResource)
		}
		result.Tasks = append(result.Tasks, planned)
	}
	for _, anchor := range plan.Anchors {
		result.Anchors = append(result.Anchors, framework.XPUHardAnchor{
			Group:       framework.XPUHardGroupRef{Job: anchor.Group.Job, SubJob: anchor.Group.SubJob},
			Class:       anchor.Class,
			LocalDomain: copyLocal(anchor.LocalDomain),
			Fabric:      copyFabric(anchor.Fabric),
		})
	}
	return result, nil
}
