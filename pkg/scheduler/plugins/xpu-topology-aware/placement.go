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
	"time"

	"k8s.io/apimachinery/pkg/types"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// HardGroupRef distinguishes a Job-wide policy from one SubGroup policy.
// It is a Session-local identity; it is never written to the Pod annotation.
type HardGroupRef struct {
	Job    api.JobID
	SubJob api.SubJobID
}

// HardPlanPolicy is one already canonical policy and its authoring scope.
// Pod selectors are evaluated against Task.Pod by the planner.
type HardPlanPolicy struct {
	Policy api.CanonicalDeviceTopologyPolicy
	Group  HardGroupRef
}

// HardPlanTask is one new Allocate operation in the complete admission wave.
// NodeName is fixed by ordinary scheduling; PR11 will construct this view from
// the trial or winning Statement rather than letting this planner pick a Node.
type HardPlanTask struct {
	Task     *api.TaskInfo
	NodeName string
	Policies []HardPlanPolicy
}

// HardGroupAnchor is recovered from already bound Pods by a later phase. PR10
// accepts it as a fixed constraint but does not read or write Pod metadata.
type HardGroupAnchor struct {
	Group       HardGroupRef
	Class       api.DomainClassKey
	LocalDomain *api.LocalDomainKey
	Fabric      *api.FabricKey
}

// HardPlanInput contains only caller-owned, read-only observations. The caller
// must check ordinary Node feasibility and NodeUID pairing for each placement.
// Occupied includes known bound assignments and other Session-local selections.
// The snapshot has no allocation ledger, so the final path must still validate
// selected keys with an exact-capable Provider before Bind.
type HardPlanInput struct {
	Tasks     []HardPlanTask
	Snapshot  *api.DeviceTopologySnapshot
	Anchors   []HardGroupAnchor
	Occupied  []api.DeviceKey
	Now       time.Time
	MaxStates int
}

// HardPolicyPlacement identifies the concrete instance satisfying one policy.
// Exactly one of LocalDomain and Fabric is non-nil.
type HardPolicyPlacement struct {
	Class       api.DomainClassKey
	Group       *HardGroupRef
	LocalDomain *api.LocalDomainKey
	Fabric      *api.FabricKey
}

// HardResourcePlan is one target resource's exact, canonical selection.
type HardResourcePlan struct {
	Request    api.XPUResourceRequest
	DeviceKeys []api.DeviceKey
	Placements []HardPolicyPlacement
}

// HardTaskPlan covers every target resource of one Task on its fixed Node.
type HardTaskPlan struct {
	TaskID    api.TaskID
	PodUID    types.UID
	NodeName  string
	Resources []HardResourcePlan
}

// HardPlan is a detached value result. A successful plan covers all input
// Tasks; it neither reserves keys nor authorizes Bind.
type HardPlan struct {
	Tasks   []HardTaskPlan
	Anchors []HardGroupAnchor
}

// HardPlanFailure is a stable scheduling reason plus diagnostic detail.
// Details may include identities for logs, but must not be used as metric labels.
type HardPlanFailure struct {
	Reason string
	Detail string
}

func (f *HardPlanFailure) Error() string { return f.Detail }

const (
	hardPlanInvalidReason       = api.XPUTopologyPolicyInvalidReason
	hardPlanUnavailableReason   = "XPUTopologyUnavailable"
	hardPlanUnsatisfiableReason = "XPUTopologyUnsatisfiable"
	hardPlanDefaultMaxStates    = 20000
)

type hardGroupPolicyKey struct {
	Group HardGroupRef
	Class api.DomainClassKey
}

type hardInstance struct {
	local  *api.LocalDomainKey
	fabric *api.FabricKey
}
