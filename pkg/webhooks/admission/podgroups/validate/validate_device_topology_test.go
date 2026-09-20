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

package validate

import (
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/features"
)

func TestValidateDeviceTopologyUpdateChangesApplyToFuturePods(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.XPUTopologyAwareScheduling, true)

	old := podGroupWithDeviceTopology("node-a")

	canonicalNoop := old.DeepCopy()
	canonicalNoop.Spec.DeviceTopology.Policies[0].ApplyTo = schedulingv1beta1.DeviceTopologyApplyToPod
	canonicalNoop.Spec.DeviceTopology.Policies[0].Mode = schedulingv1beta1.HardDeviceTopologyMode
	response := Validate(podGroupAdmissionReview(t, admissionv1.Update, canonicalNoop, old))
	if !response.Allowed {
		t.Fatalf("canonical no-op update was rejected: %s", admissionMessage(response))
	}

	mutated := old.DeepCopy()
	mutated.Spec.DeviceTopology.Policies[0].Domain.DomainClass = "node-b"
	response = Validate(podGroupAdmissionReview(t, admissionv1.Update, mutated, old))
	if !response.Allowed {
		t.Fatalf("semantic update for future Pods was rejected: %s", admissionMessage(response))
	}

	deleted := old.DeepCopy()
	deleted.Spec.DeviceTopology = nil
	response = Validate(podGroupAdmissionReview(t, admissionv1.Update, deleted, old))
	if !response.Allowed {
		t.Fatalf("policy deletion for future Pods was rejected: %s", admissionMessage(response))
	}

	oldWithSubGroup := old.DeepCopy()
	oldWithSubGroup.Spec.SubGroupPolicy = []schedulingv1beta1.SubGroupPolicySpec{{
		Name:           "workers",
		MatchLabelKeys: []string{"role"},
		DeviceTopology: old.Spec.DeviceTopology.DeepCopy(),
	}}
	mutatedSubGroup := oldWithSubGroup.DeepCopy()
	mutatedSubGroup.Spec.SubGroupPolicy[0].MatchLabelKeys = []string{"rack"}
	response = Validate(podGroupAdmissionReview(t, admissionv1.Update, mutatedSubGroup, oldWithSubGroup))
	if !response.Allowed {
		t.Fatalf("subgroup topology update for future Pods was rejected: %s", admissionMessage(response))
	}
}

func TestValidateDeviceTopologyAdmissionUsesAuthoringShapeNotSchedulerCatalog(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.XPUTopologyAwareScheduling, true)

	pg := podGroupWithDeviceTopology("class-not-in-local-catalog")
	response := Validate(podGroupAdmissionReview(t, admissionv1.Create, pg, nil))
	if !response.Allowed {
		t.Fatalf("admission rejected unknown scheduler catalog class: %s", admissionMessage(response))
	}
}

func TestValidateDeviceTopologyFeatureGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.XPUTopologyAwareScheduling, false)
	old := podGroupWithDeviceTopology("node-a")

	response := Validate(podGroupAdmissionReview(t, admissionv1.Create, old, nil))
	if response.Allowed || !strings.Contains(admissionMessage(response), "XPUTopologyAwareScheduling is disabled") {
		t.Fatalf("disabled feature accepted create: %#v", response)
	}

	deleted := old.DeepCopy()
	deleted.Spec.DeviceTopology = nil
	response = Validate(podGroupAdmissionReview(t, admissionv1.Update, deleted, old))
	if !response.Allowed {
		t.Fatalf("disabled feature rejected policy deletion: %s", admissionMessage(response))
	}

	mutated := old.DeepCopy()
	mutated.Spec.DeviceTopology.Policies[0].Domain.DomainClass = "node-b"
	response = Validate(podGroupAdmissionReview(t, admissionv1.Update, mutated, old))
	if response.Allowed || !strings.Contains(admissionMessage(response), "only policy deletion is allowed") {
		t.Fatalf("disabled feature accepted semantic update: %#v", response)
	}
}

func TestValidateDeviceTopologyStrictRawDecoding(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.XPUTopologyAwareScheduling, true)

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "unknown nested policy field",
			raw:  `{"apiVersion":"scheduling.volcano.sh/v1beta1","kind":"PodGroup","metadata":{"name":"train","namespace":"default"},"spec":{"deviceTopology":{"policies":[{"resourceName":"example.com/gpu","domain":{"scope":"Node","domainClass":"node-a"},"extra":"not-allowed"}]}}}`,
			want: "unknown field",
		},
		{
			name: "duplicate nested policy key",
			raw:  `{"apiVersion":"scheduling.volcano.sh/v1beta1","kind":"PodGroup","metadata":{"name":"train","namespace":"default"},"spec":{"deviceTopology":{"policies":[{"resourceName":"example.com/gpu","domain":{"scope":"Node","domainClass":"node-a","domainClass":"node-b"}}]}}}`,
			want: "duplicate JSON object key",
		},
		{
			name: "invalid policy container type",
			raw:  `{"apiVersion":"scheduling.volcano.sh/v1beta1","kind":"PodGroup","metadata":{"name":"train","namespace":"default"},"spec":{"deviceTopology":{"policies":{}}}}`,
			want: "cannot unmarshal object into Go struct field",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := Validate(rawPodGroupAdmissionReview(admissionv1.Create, []byte(test.raw), nil))
			if response.Allowed || !strings.Contains(admissionMessage(response), test.want) {
				t.Fatalf("strict raw response = %#v, want rejection containing %q", response, test.want)
			}
		})
	}
}

func TestValidatePodGroupStatusSubresourceSkipsAuthoringValidation(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.XPUTopologyAwareScheduling, false)
	response := Validate(admissionv1.AdmissionReview{Request: &admissionv1.AdmissionRequest{
		Operation:   admissionv1.Update,
		SubResource: "status",
		Resource:    podGroupResource(),
		Object:      runtime.RawExtension{Raw: []byte(`not valid JSON`)},
	}})
	if !response.Allowed {
		t.Fatalf("status subresource update should bypass authoring validation: %s", admissionMessage(response))
	}
}

func podGroupWithDeviceTopology(class string) *schedulingv1beta1.PodGroup {
	return &schedulingv1beta1.PodGroup{
		TypeMeta: metav1.TypeMeta{APIVersion: schedulingv1beta1.SchemeGroupVersion.String(), Kind: "PodGroup"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "train",
		},
		Spec: schedulingv1beta1.PodGroupSpec{DeviceTopology: &schedulingv1beta1.DeviceTopologySpec{
			Policies: []schedulingv1beta1.DeviceTopologyPolicy{{
				ResourceName: "example.com/gpu",
				Domain: schedulingv1beta1.DeviceTopologyDomainSelector{
					Scope:       schedulingv1beta1.DeviceTopologyDomainScopeNode,
					DomainClass: schedulingv1beta1.DeviceTopologyDomainClass(class),
				},
			}},
		}},
	}
}

func podGroupAdmissionReview(t *testing.T, operation admissionv1.Operation, object, oldObject *schedulingv1beta1.PodGroup) admissionv1.AdmissionReview {
	t.Helper()
	raw, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("marshal new PodGroup: %v", err)
	}
	var oldRaw []byte
	if oldObject != nil {
		oldRaw, err = json.Marshal(oldObject)
		if err != nil {
			t.Fatalf("marshal old PodGroup: %v", err)
		}
	}
	return rawPodGroupAdmissionReview(operation, raw, oldRaw)
}

func rawPodGroupAdmissionReview(operation admissionv1.Operation, raw, oldRaw []byte) admissionv1.AdmissionReview {
	return admissionv1.AdmissionReview{Request: &admissionv1.AdmissionRequest{
		Operation: operation,
		Name:      "train",
		Resource:  podGroupResource(),
		Object:    runtime.RawExtension{Raw: raw},
		OldObject: runtime.RawExtension{Raw: oldRaw},
	}}
}

func podGroupResource() metav1.GroupVersionResource {
	return metav1.GroupVersionResource{
		Group:    schedulingv1beta1.SchemeGroupVersion.Group,
		Version:  schedulingv1beta1.SchemeGroupVersion.Version,
		Resource: "podgroups",
	}
}

func admissionMessage(response *admissionv1.AdmissionResponse) string {
	if response == nil || response.Result == nil {
		return ""
	}
	return response.Result.Message
}
