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
	"k8s.io/apimachinery/pkg/util/validation/field"

	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// CanonicalizeDeviceTopologyForAuthoring validates only the typed public
// policy contract. Admission deliberately does not read the scheduler-local
// catalog, so an unknown class remains a scheduler-side fail-closed outcome
// instead of being accepted or silently removed here.
func CanonicalizeDeviceTopologyForAuthoring(spec *schedulingv1beta1.DeviceTopologySpec, source AuthoringSource) (CanonicalDeviceTopologySpec, PolicyFingerprint, field.ErrorList) {
	return CanonicalizeDeviceTopology(spec, source, acceptAllDomainClasses{})
}

// CanonicalizeInternalDeviceTopologyForAuthoring is the scheduler-internal
// counterpart of CanonicalizeDeviceTopologyForAuthoring. PodGroups in
// JobInfo use the internal API type, while admission receives v1beta1. Both
// paths must produce the same canonical policy and fingerprint.
func CanonicalizeInternalDeviceTopologyForAuthoring(spec *scheduling.DeviceTopologySpec, source AuthoringSource) (CanonicalDeviceTopologySpec, PolicyFingerprint, field.ErrorList) {
	return CanonicalizeDeviceTopologyForAuthoring(externalDeviceTopologySpec(spec), source)
}

type acceptAllDomainClasses struct{}

func (acceptAllDomainClasses) HasDomainClass(DomainClassKey) bool {
	return true
}

func externalDeviceTopologySpec(in *scheduling.DeviceTopologySpec) *schedulingv1beta1.DeviceTopologySpec {
	if in == nil {
		return nil
	}

	out := &schedulingv1beta1.DeviceTopologySpec{Policies: make([]schedulingv1beta1.DeviceTopologyPolicy, len(in.Policies))}
	for index, policy := range in.Policies {
		out.Policies[index] = schedulingv1beta1.DeviceTopologyPolicy{
			ResourceName: policy.ResourceName,
			ApplyTo:      schedulingv1beta1.DeviceTopologyApplyTo(policy.ApplyTo),
			Mode:         schedulingv1beta1.DeviceTopologyMode(policy.Mode),
			Domain: schedulingv1beta1.DeviceTopologyDomainSelector{
				Scope:       schedulingv1beta1.DeviceTopologyDomainScope(policy.Domain.Scope),
				DomainClass: schedulingv1beta1.DeviceTopologyDomainClass(policy.Domain.DomainClass),
			},
		}
		if policy.PodSelector != nil {
			out.Policies[index].PodSelector = policy.PodSelector.DeepCopy()
		}
	}

	return out
}

func cloneCanonicalDeviceTopologySpec(in CanonicalDeviceTopologySpec) CanonicalDeviceTopologySpec {
	if len(in.Policies) == 0 {
		return CanonicalDeviceTopologySpec{}
	}
	out := CanonicalDeviceTopologySpec{Policies: make([]CanonicalDeviceTopologyPolicy, len(in.Policies))}
	for index, policy := range in.Policies {
		out.Policies[index] = policy
		if len(policy.PodSelector.MatchLabels) > 0 {
			out.Policies[index].PodSelector.MatchLabels = append([]CanonicalLabel(nil), policy.PodSelector.MatchLabels...)
		}
		if len(policy.PodSelector.MatchExpressions) == 0 {
			continue
		}
		out.Policies[index].PodSelector.MatchExpressions = make([]CanonicalLabelSelectorRequirement, len(policy.PodSelector.MatchExpressions))
		for requirementIndex, requirement := range policy.PodSelector.MatchExpressions {
			out.Policies[index].PodSelector.MatchExpressions[requirementIndex] = requirement
			out.Policies[index].PodSelector.MatchExpressions[requirementIndex].Values = append([]string(nil), requirement.Values...)
		}
	}
	return out
}
