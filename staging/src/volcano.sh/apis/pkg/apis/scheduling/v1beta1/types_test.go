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

package v1beta1_test

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	scheduling "volcano.sh/apis/pkg/apis/scheduling"
	v1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

func TestDeviceTopologyDeepCopyAndConversion(t *testing.T) {
	spec := &v1beta1.PodGroupSpec{
		DeviceTopology: topologySpec("frontend", true),
		SubGroupPolicy: []v1beta1.SubGroupPolicySpec{{
			Name:           "worker",
			DeviceTopology: topologySpec("backend", false),
		}},
	}

	copy := spec.DeepCopy()
	copy.DeviceTopology.Policies[0].PodSelector.MatchLabels["component"] = "changed"
	copy.SubGroupPolicy[0].DeviceTopology.Policies[0].Domain.DomainClass = "changed"
	if got := spec.DeviceTopology.Policies[0].PodSelector.MatchLabels["component"]; got != "frontend" {
		t.Fatalf("DeepCopy aliased PodSelector labels: got %q", got)
	}
	if got := spec.SubGroupPolicy[0].DeviceTopology.Policies[0].Domain.DomainClass; got != "backend" {
		t.Fatalf("DeepCopy aliased SubGroupPolicy DeviceTopology: got %q", got)
	}

	scheme := runtime.NewScheme()
	if err := scheduling.AddToScheme(scheme); err != nil {
		t.Fatalf("add internal scheduling types: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1 scheduling types: %v", err)
	}

	internal := &scheduling.PodGroupSpec{}
	if err := scheme.Convert(spec, internal, nil); err != nil {
		t.Fatalf("convert to internal PodGroupSpec: %v", err)
	}
	if got := internal.DeviceTopology.Policies[0].Domain.DomainClass; got != "frontend" {
		t.Fatalf("PodGroup DeviceTopology was not converted: got %q", got)
	}
	if got := internal.SubGroupPolicy[0].DeviceTopology.Policies[0].Domain.DomainClass; got != "backend" {
		t.Fatalf("SubGroupPolicy DeviceTopology was not converted: got %q", got)
	}

	roundTrip := &v1beta1.PodGroupSpec{}
	if err := scheme.Convert(internal, roundTrip, nil); err != nil {
		t.Fatalf("convert PodGroupSpec back to v1beta1: %v", err)
	}
	if got := roundTrip.DeviceTopology.Policies[0].ResourceName; got != v1.ResourceName("example.com/accelerator") {
		t.Fatalf("round-trip resource name: got %q", got)
	}
	if got := roundTrip.SubGroupPolicy[0].DeviceTopology.Policies[0].ApplyTo; got != v1beta1.DeviceTopologyApplyToPod {
		t.Fatalf("round-trip applyTo: got %q", got)
	}
}

func topologySpec(component string, withPodSelector bool) *v1beta1.DeviceTopologySpec {
	policy := v1beta1.DeviceTopologyPolicy{
		ResourceName: v1.ResourceName("example.com/accelerator"),
		ApplyTo:      v1beta1.DeviceTopologyApplyToPod,
		Mode:         v1beta1.SoftDeviceTopologyMode,
		Domain: v1beta1.DeviceTopologyDomainSelector{
			Scope:       v1beta1.DeviceTopologyDomainScopeNode,
			DomainClass: v1beta1.DeviceTopologyDomainClass(component),
		},
	}
	if withPodSelector {
		policy.PodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"component": component}}
	}
	return &v1beta1.DeviceTopologySpec{Policies: []v1beta1.DeviceTopologyPolicy{policy}}
}
