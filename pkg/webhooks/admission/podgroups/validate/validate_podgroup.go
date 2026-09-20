/*
Copyright 2021 The Volcano Authors.

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
	"fmt"
	"sort"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	whv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/features"
	schedulerapi "volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/schema"
	"volcano.sh/volcano/pkg/webhooks/util"
)

func init() {
	router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path:   "/podgroups/validate",
	Func:   Validate,
	Config: config,

	ValidatingConfig: &whv1.ValidatingWebhookConfiguration{
		Webhooks: []whv1.ValidatingWebhook{{
			Name: "validatepodgroup.volcano.sh",
			Rules: []whv1.RuleWithOperations{
				{
					Operations: []whv1.OperationType{whv1.Create, whv1.Update},
					Rule: whv1.Rule{
						APIGroups:   []string{schedulingv1beta1.SchemeGroupVersion.Group},
						APIVersions: []string{schedulingv1beta1.SchemeGroupVersion.Version},
						Resources:   []string{"podgroups"},
					},
				},
			},
		}},
	},
}

var config = &router.AdmissionServiceConfig{}

// Validate validates the PodGroup object when creating or updating it
func Validate(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("Validating %s PodGroup %s", ar.Request.Operation, ar.Request.Name)
	if ar.Request.SubResource == "status" {
		return &admissionv1.AdmissionResponse{Allowed: true}
	}

	podgroup, err := schema.DecodePodGroupStrict(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return util.ToAdmissionResponse(err)
	}

	var errMsg string
	switch ar.Request.Operation {
	case admissionv1.Create:
		errMsg = validatePodGroup(podgroup)
		if errMsg == "" {
			errMsg = validateCreatedDeviceTopology(podgroup)
		}
	case admissionv1.Update:
		oldPodGroup, decodeErr := schema.DecodePodGroupStrict(ar.Request.OldObject, ar.Request.Resource)
		if decodeErr != nil {
			return util.ToAdmissionResponse(decodeErr)
		}
		// The pre-existing checks were CREATE-only. UPDATE validates the
		// authoring shape of the new typed policy without freezing an active
		// PodGroup: a changed policy applies to future, unbound Pods only.
		errMsg = validateUpdatedDeviceTopology(oldPodGroup, podgroup)
	default:
		errMsg = fmt.Sprintf("unsupported operation %s", ar.Request.Operation)
	}

	if errMsg != "" {
		return &admissionv1.AdmissionResponse{
			Allowed: false,
			Result:  &metav1.Status{Message: errMsg},
		}
	}

	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}
}

func validateCreatedDeviceTopology(pg *schedulingv1beta1.PodGroup) string {
	if !hasDeviceTopologyPolicy(pg) {
		return ""
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.XPUTopologyAwareScheduling) {
		return "XPUTopologyPolicyInvalid: XPUTopologyAwareScheduling is disabled"
	}
	_, err := topologyPolicySignature(pg)
	return err
}

func validateUpdatedDeviceTopology(oldPG, newPG *schedulingv1beta1.PodGroup) string {
	newHasPolicy := hasDeviceTopologyPolicy(newPG)
	oldHasPolicy := hasDeviceTopologyPolicy(oldPG)
	if !utilfeature.DefaultFeatureGate.Enabled(features.XPUTopologyAwareScheduling) {
		// A disabled admission process does not reinterpret persisted intent. It
		// accepts a no-op or deletion, but rejects addition and semantic change.
		if !newHasPolicy {
			return ""
		}
		if !oldHasPolicy {
			return "XPUTopologyPolicyInvalid: XPUTopologyAwareScheduling is disabled"
		}
		oldSignature, oldErr := topologyPolicySignature(oldPG)
		if oldErr != "" {
			return oldErr
		}
		newSignature, newErr := topologyPolicySignature(newPG)
		if newErr != "" {
			return newErr
		}
		if oldSignature != newSignature {
			return "XPUTopologyPolicyInvalid: XPUTopologyAwareScheduling is disabled; only policy deletion is allowed"
		}
		return ""
	}

	// Advisory policy updates are intentionally forward-looking. Existing
	// Bound/Running Pods are not rescheduled; the scheduler refreshes its
	// JobInfo view from the new persisted PodGroup for future Pending Pods.
	_, err := topologyPolicySignature(newPG)
	return err
}

// topologyPolicySignature validates the public policy shape and returns a
// stable identity for the PodGroup field and every subgroup topology view. It
// intentionally uses the admission-only canonicalizer, which does not consult
// the scheduler-local catalog. The subgroup position and membership fields
// are included because changing either can change which Pods receive an
// otherwise identical DeviceTopology policy.
func topologyPolicySignature(pg *schedulingv1beta1.PodGroup) (string, string) {
	parts := make([]string, 0, 1+len(pg.Spec.SubGroupPolicy))
	if pg.Spec.DeviceTopology != nil && len(pg.Spec.DeviceTopology.Policies) > 0 {
		_, fingerprint, errs := schedulerapi.CanonicalizeDeviceTopologyForAuthoring(pg.Spec.DeviceTopology, schedulerapi.PodGroupAuthoringSource)
		if len(errs) > 0 {
			return "", fmt.Sprintf("XPUTopologyPolicyInvalid: deviceTopology: %s", errs.ToAggregate().Error())
		}
		parts = append(parts, "podgroup="+string(fingerprint))
	}
	for index, policy := range pg.Spec.SubGroupPolicy {
		if policy.DeviceTopology == nil || len(policy.DeviceTopology.Policies) == 0 {
			continue
		}
		_, fingerprint, errs := schedulerapi.CanonicalizeDeviceTopologyForAuthoring(policy.DeviceTopology, schedulerapi.SubGroupAuthoringSource)
		if len(errs) > 0 {
			return "", fmt.Sprintf("XPUTopologyPolicyInvalid: subGroupPolicy[%d].deviceTopology: %s", index, errs.ToAggregate().Error())
		}
		context, marshalErr := json.Marshal(subGroupTopologyContext{
			Index:                     index,
			Name:                      policy.Name,
			NetworkTopology:           policy.NetworkTopology,
			SubGroupSize:              policy.SubGroupSize,
			MinSubGroups:              policy.MinSubGroups,
			LabelSelector:             policy.LabelSelector,
			MatchLabelKeys:            policy.MatchLabelKeys,
			DeviceTopologyFingerprint: fingerprint,
		})
		if marshalErr != nil {
			return "", fmt.Sprintf("XPUTopologyPolicyInvalid: subGroupPolicy[%d]: %v", index, marshalErr)
		}
		parts = append(parts, "subgroup="+string(context))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x00"), ""
}

type subGroupTopologyContext struct {
	Index                     int                                    `json:"index"`
	Name                      string                                 `json:"name"`
	NetworkTopology           *schedulingv1beta1.NetworkTopologySpec `json:"networkTopology,omitempty"`
	SubGroupSize              *int32                                 `json:"subGroupSize,omitempty"`
	MinSubGroups              *int32                                 `json:"minSubGroups,omitempty"`
	LabelSelector             *metav1.LabelSelector                  `json:"labelSelector,omitempty"`
	MatchLabelKeys            []string                               `json:"matchLabelKeys,omitempty"`
	DeviceTopologyFingerprint schedulerapi.PolicyFingerprint         `json:"deviceTopologyFingerprint"`
}

func hasDeviceTopologyPolicy(pg *schedulingv1beta1.PodGroup) bool {
	if pg.Spec.DeviceTopology != nil && len(pg.Spec.DeviceTopology.Policies) > 0 {
		return true
	}
	for _, policy := range pg.Spec.SubGroupPolicy {
		if policy.DeviceTopology != nil && len(policy.DeviceTopology.Policies) > 0 {
			return true
		}
	}
	return false
}

// validatePodGroup validates a PodGroup when it's being created
func validatePodGroup(pg *schedulingv1beta1.PodGroup) string {
	var errs []string

	if msg := checkQueueState(pg.Spec.Queue); msg != "" {
		errs = append(errs, strings.TrimSpace(msg))
	}
	if msg := validateNetworkTopology(pg.Spec.NetworkTopology, pg.Spec.SubGroupPolicy); msg != "" {
		errs = append(errs, strings.TrimSpace(msg))
	}

	return strings.Join(errs, "; ")
}

// checkQueueState verifies if the queue exists and is in the open state
func checkQueueState(queueName string) string {
	if queueName == "" {
		return ""
	}

	queue, err := config.QueueLister.Get(queueName)
	if err != nil {
		return fmt.Sprintf("unable to find queue: %s", err.Error())
	}

	if queue.Status.State != schedulingv1beta1.QueueStateOpen {
		return fmt.Sprintf("can only submit PodGroup to queue with state `Open`, queue `%s` status is `%s`. ",
			queue.Name, queue.Status.State)
	}

	return ""
}

func validateNetworkTopology(networkTopology *schedulingv1beta1.NetworkTopologySpec, policies []schedulingv1beta1.SubGroupPolicySpec) string {
	var errs []string
	if networkTopology != nil && networkTopology.HighestTierAllowed != nil && networkTopology.HighestTierName != "" {
		errs = append(errs, "must not specify 'highestTierAllowed' and 'highestTierName' in networkTopology simultaneously.")
	}
	for _, policy := range policies {
		if policy.NetworkTopology != nil && policy.NetworkTopology.HighestTierAllowed != nil && policy.NetworkTopology.HighestTierName != "" {
			errs = append(errs, fmt.Sprintf("in subGroupPolicy '%s': must not specify 'highestTierAllowed' and 'highestTierName' in networkTopology simultaneously.", policy.Name))
			break
		}
	}
	return strings.Join(errs, " ")
}
