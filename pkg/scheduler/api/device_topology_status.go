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

package api

import (
	"strings"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

const (
	// XPUTopologyPolicyInvalidReason means that a scheduler-side compiler could
	// not consume an otherwise persisted typed policy.
	XPUTopologyPolicyInvalidReason = "XPUTopologyPolicyInvalid"
	// XPUTopologyDomainClassUnknownReason means the policy class is absent from
	// the fixed scheduler catalog for its resource and scope.
	XPUTopologyDomainClassUnknownReason = "XPUTopologyDomainClassUnknown"
	// XPUTopologyDomainClassUnsupportedReason means a configured Provider
	// cannot supply the requested known class.
	XPUTopologyDomainClassUnsupportedReason = "XPUTopologyDomainClassUnsupported"
	// XPUTopologyDataNotReadyReason means an xPU policy needs topology facts
	// which have not been published for the observed Node incarnation.
	XPUTopologyDataNotReadyReason = "XPUTopologyDataNotReady"
	// XPUTopologyStaleReason means the Provider facts passed their explicit
	// freshness deadline. Soft policies lose only their preference in this case.
	XPUTopologyStaleReason = "XPUTopologyStale"
	// XPUTopologyUnsupportedPodRequestReason means a policy-selected Pod does
	// not use the M2 single-container, integral request=limit shape.
	XPUTopologyUnsupportedPodRequestReason = "XPUTopologyUnsupportedPodRequest"
	// XPUTopologyPolicyConflictReason means an authoring controller found
	// conflicting policy input. The scheduler must leave this blocker intact
	// until its author clears it through the PodGroup status subresource.
	XPUTopologyPolicyConflictReason = "XPUTopologyPolicyConflict"
	// XPUTopologyResolvedReason clears an authoring blocker after its owner has
	// confirmed the underlying issue is gone.
	XPUTopologyResolvedReason = "XPUTopologyResolved"
)

// IsXPUTopologyAuthoringBlocker reports whether condition is a still-active
// authoring failure. It is deliberately limited to the two reasons that are
// owned outside a scheduling Session; runtime topology reasons remain owned
// by the scheduler outcome path.
func IsXPUTopologyAuthoringBlocker(condition scheduling.PodGroupCondition) bool {
	if condition.Type != scheduling.PodGroupUnschedulableType || condition.Status != v1.ConditionTrue {
		return false
	}
	return condition.Reason == XPUTopologyPolicyConflictReason
}

// MayReplaceXPUTopologyAuthoringBlocker returns whether a new Unschedulable
// condition may replace an active authoring blocker. Dynamic scheduler
// outcomes must not hide it. The explicit False/Resolved transition is kept
// for the authoring owner after a status-subresource conflict retry.
func MayReplaceXPUTopologyAuthoringBlocker(next scheduling.PodGroupCondition) bool {
	return next.Type == scheduling.PodGroupUnschedulableType &&
		next.Status == v1.ConditionFalse &&
		next.Reason == XPUTopologyResolvedReason
}

// isXPUTopologyRuntimeBlocker reports an active scheduler-owned xPU outcome.
// Unlike authoring blockers, another xPU outcome may supersede it, but a
// generic gang condition must not hide it before the API status writer runs.
func isXPUTopologyRuntimeBlocker(condition scheduling.PodGroupCondition) bool {
	return condition.Type == scheduling.PodGroupUnschedulableType &&
		condition.Status == v1.ConditionTrue &&
		strings.HasPrefix(condition.Reason, "XPU") &&
		!IsXPUTopologyAuthoringBlocker(condition)
}

func mayReplaceXPUTopologyRuntimeBlocker(next scheduling.PodGroupCondition) bool {
	if next.Type != scheduling.PodGroupUnschedulableType {
		return false
	}
	return (next.Status == v1.ConditionTrue && strings.HasPrefix(next.Reason, "XPU")) ||
		(next.Status == v1.ConditionFalse && next.Reason == XPUTopologyResolvedReason)
}

// MergePodGroupConditions preserves conditions that an updater did not own
// while applying one desired condition per type. In particular, an active xPU
// authoring blocker wins over a dynamic scheduler Unschedulable reason.
func MergePodGroupConditions(current, desired []scheduling.PodGroupCondition) []scheduling.PodGroupCondition {
	merged := append([]scheduling.PodGroupCondition(nil), current...)
	for _, next := range desired {
		index := -1
		for i, existing := range merged {
			if existing.Type == next.Type {
				index = i
				switch {
				case IsXPUTopologyAuthoringBlocker(existing) && !MayReplaceXPUTopologyAuthoringBlocker(next):
					index = -2
				case isXPUTopologyRuntimeBlocker(existing) && !mayReplaceXPUTopologyRuntimeBlocker(next):
					index = -2
				}
				break
			}
		}
		switch index {
		case -1:
			merged = append(merged, next)
		case -2:
			continue
		default:
			merged[index] = next
		}
	}
	return merged
}

// MergePodGroupStatus takes the scheduler's desired counters/phase and merges
// conditions from the latest API object. It avoids a stale scheduling Session
// overwriting a concurrently written xPU authoring blocker.
func MergePodGroupStatus(current, desired scheduling.PodGroupStatus) scheduling.PodGroupStatus {
	merged := desired
	merged.Conditions = MergePodGroupConditions(current.Conditions, desired.Conditions)
	return merged
}

// MergePodGroupStatusV1beta1 applies the same status-owner rules at the API
// client boundary, where the generated client uses the external v1beta1 type.
func MergePodGroupStatusV1beta1(current, desired schedulingv1beta1.PodGroupStatus) schedulingv1beta1.PodGroupStatus {
	merged := desired
	merged.Conditions = append([]schedulingv1beta1.PodGroupCondition(nil), current.Conditions...)
	for _, next := range desired.Conditions {
		index := -1
		for i, existing := range merged.Conditions {
			if existing.Type != next.Type {
				continue
			}
			index = i
			switch {
			case isXPUTopologyAuthoringBlockerV1beta1(existing) && !mayReplaceXPUTopologyAuthoringBlockerV1beta1(next):
				index = -2
			case isXPUTopologyRuntimeBlockerV1beta1(existing) && !mayReplaceXPUTopologyRuntimeBlockerV1beta1(next):
				index = -2
			}
			break
		}
		switch index {
		case -1:
			merged.Conditions = append(merged.Conditions, next)
		case -2:
			continue
		default:
			merged.Conditions[index] = next
		}
	}
	return merged
}

func isXPUTopologyAuthoringBlockerV1beta1(condition schedulingv1beta1.PodGroupCondition) bool {
	return condition.Type == schedulingv1beta1.PodGroupUnschedulableType &&
		condition.Status == v1.ConditionTrue &&
		condition.Reason == XPUTopologyPolicyConflictReason
}

func mayReplaceXPUTopologyAuthoringBlockerV1beta1(next schedulingv1beta1.PodGroupCondition) bool {
	return next.Type == schedulingv1beta1.PodGroupUnschedulableType &&
		next.Status == v1.ConditionFalse && next.Reason == XPUTopologyResolvedReason
}

func isXPUTopologyRuntimeBlockerV1beta1(condition schedulingv1beta1.PodGroupCondition) bool {
	return condition.Type == schedulingv1beta1.PodGroupUnschedulableType &&
		condition.Status == v1.ConditionTrue &&
		strings.HasPrefix(condition.Reason, "XPU") &&
		!isXPUTopologyAuthoringBlockerV1beta1(condition)
}

func mayReplaceXPUTopologyRuntimeBlockerV1beta1(next schedulingv1beta1.PodGroupCondition) bool {
	if next.Type != schedulingv1beta1.PodGroupUnschedulableType {
		return false
	}
	return (next.Status == v1.ConditionTrue && strings.HasPrefix(next.Reason, "XPU")) ||
		(next.Status == v1.ConditionFalse && next.Reason == XPUTopologyResolvedReason)
}
