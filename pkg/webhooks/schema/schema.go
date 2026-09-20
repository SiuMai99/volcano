/*
Copyright 2019 The Volcano Authors.

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

package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	flowv1alpha1 "volcano.sh/apis/pkg/apis/flow/v1alpha1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	hypernodev1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
)

func init() {
	addToScheme(scheme)
}

var scheme = runtime.NewScheme()

// Codecs is for retrieving serializers for the supported wire formats
// and conversion wrappers to define preferred internal and external versions.
var Codecs = serializer.NewCodecFactory(scheme)

func addToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(admissionv1.AddToScheme(scheme))
}

// DecodeJob decodes the job using deserializer from the raw object.
func DecodeJob(object runtime.RawExtension, resource metav1.GroupVersionResource) (*batchv1alpha1.Job, error) {
	jobResource := metav1.GroupVersionResource{Group: batchv1alpha1.SchemeGroupVersion.Group, Version: batchv1alpha1.SchemeGroupVersion.Version, Resource: "jobs"}
	raw := object.Raw
	job := batchv1alpha1.Job{}

	if resource != jobResource {
		klog.Errorf("expect resource to be %s", jobResource)
		return &job, fmt.Errorf("expect resource to be %s", jobResource)
	}

	deserializer := Codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(raw, nil, &job); err != nil {
		return &job, err
	}
	klog.V(3).Infof("decoded job uid=%s namespace=%s name=%s", job.UID, job.Namespace, job.Name)
	klog.V(7).Infof("the job struct is %+v", job)

	return &job, nil
}

func DecodeCronJob(object runtime.RawExtension, resource metav1.GroupVersionResource) (*batchv1alpha1.CronJob, error) {
	cronjobResource := metav1.GroupVersionResource{Group: batchv1alpha1.SchemeGroupVersion.Group, Version: batchv1alpha1.SchemeGroupVersion.Version, Resource: "cronjobs"}
	raw := object.Raw
	cronjob := batchv1alpha1.CronJob{}

	if resource != cronjobResource {
		klog.Errorf("expect resource to be %s", cronjobResource)
		return &cronjob, fmt.Errorf("expect resource to be %s", cronjobResource)
	}

	deserializer := Codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(raw, nil, &cronjob); err != nil {
		return &cronjob, err
	}
	klog.V(3).Infof("decoded cronjob uid=%s namespace=%s name=%s", cronjob.UID, cronjob.Namespace, cronjob.Name)
	klog.V(7).Infof("the cronjob struct is %+v", cronjob)

	return &cronjob, nil
}

func DecodePod(object runtime.RawExtension, resource metav1.GroupVersionResource) (*v1.Pod, error) {
	podResource := metav1.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	raw := object.Raw
	pod := v1.Pod{}

	if resource != podResource {
		klog.Errorf("expect resource to be %s", podResource)
		return &pod, fmt.Errorf("expect resource to be %s", podResource)
	}

	deserializer := Codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(raw, nil, &pod); err != nil {
		return &pod, err
	}
	klog.V(3).Infof("decoded pod uid=%s namespace=%s name=%s", pod.UID, pod.Namespace, pod.Name)
	klog.V(7).Infof("the pod struct is %+v", pod)

	return &pod, nil
}

// DecodeQueue decodes the queue using deserializer from the raw object.
func DecodeQueue(object runtime.RawExtension, resource metav1.GroupVersionResource) (*schedulingv1beta1.Queue, error) {
	queueResource := metav1.GroupVersionResource{
		Group:    schedulingv1beta1.SchemeGroupVersion.Group,
		Version:  schedulingv1beta1.SchemeGroupVersion.Version,
		Resource: "queues",
	}

	if resource != queueResource {
		klog.Errorf("expect resource to be %s", queueResource)
		return nil, fmt.Errorf("expect resource to be %s", queueResource)
	}

	queue := schedulingv1beta1.Queue{}
	if _, _, err := Codecs.UniversalDeserializer().Decode(object.Raw, nil, &queue); err != nil {
		return nil, err
	}

	return &queue, nil
}

// DecodePodGroup decodes the podgroup using deserializer from the raw object.
func DecodePodGroup(object runtime.RawExtension, resource metav1.GroupVersionResource) (*schedulingv1beta1.PodGroup, error) {
	podgroupResource := metav1.GroupVersionResource{
		Group:    schedulingv1beta1.SchemeGroupVersion.Group,
		Version:  schedulingv1beta1.SchemeGroupVersion.Version,
		Resource: "podgroups",
	}

	if resource != podgroupResource {
		klog.Errorf("expect resource to be %s", podgroupResource)
		return nil, fmt.Errorf("expect resource to be %s", podgroupResource)
	}

	podgroup := schedulingv1beta1.PodGroup{}
	if _, _, err := Codecs.UniversalDeserializer().Decode(object.Raw, nil, &podgroup); err != nil {
		return nil, err
	}

	return &podgroup, nil
}

// DecodePodGroupStrict decodes the raw AdmissionRequest payload without
// allowing the lossy "last duplicate key wins" behavior of encoding/json.
// PodGroup device-topology authoring relies on this raw-object check: a
// decoded Go struct alone cannot tell whether an unknown field, duplicate key,
// or wrong JSON type was pruned before policy validation ran.
func DecodePodGroupStrict(object runtime.RawExtension, resource metav1.GroupVersionResource) (*schedulingv1beta1.PodGroup, error) {
	podgroupResource := metav1.GroupVersionResource{
		Group:    schedulingv1beta1.SchemeGroupVersion.Group,
		Version:  schedulingv1beta1.SchemeGroupVersion.Version,
		Resource: "podgroups",
	}
	if resource != podgroupResource {
		return nil, fmt.Errorf("expect resource to be %s", podgroupResource)
	}
	if err := rejectDuplicateJSONKeys(object.Raw); err != nil {
		return nil, fmt.Errorf("strict PodGroup decode: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(object.Raw))
	decoder.DisallowUnknownFields()
	podgroup := schedulingv1beta1.PodGroup{}
	if err := decoder.Decode(&podgroup); err != nil {
		return nil, fmt.Errorf("strict PodGroup decode: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("strict PodGroup decode: %w", err)
	}
	return &podgroup, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("contains trailing JSON value")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}

	switch delimiter {
	case '{':
		keys := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return fmt.Errorf("duplicate JSON object key %q", key)
			}
			keys[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return fmt.Errorf("expected end of object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return fmt.Errorf("expected end of array")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing interface{}
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("contains trailing JSON value")
}

// DecodeHyperNode decodes the hypernode using deserializer from the raw object.
func DecodeHyperNode(object runtime.RawExtension, resource metav1.GroupVersionResource) (*hypernodev1alpha1.HyperNode, error) {
	hypernodeResource := metav1.GroupVersionResource{
		Group:    hypernodev1alpha1.SchemeGroupVersion.Group,
		Version:  hypernodev1alpha1.SchemeGroupVersion.Version,
		Resource: "hypernodes",
	}

	if resource != hypernodeResource {
		klog.Errorf("expect resource to be %s", hypernodeResource)
		return nil, fmt.Errorf("expect resource to be %s", hypernodeResource)
	}

	hypernode := hypernodev1alpha1.HyperNode{}
	if _, _, err := Codecs.UniversalDeserializer().Decode(object.Raw, nil, &hypernode); err != nil {
		return nil, err
	}

	return &hypernode, nil
}

// DecodeJobFlow decodes the job using deserializer from the raw object.
func DecodeJobFlow(object runtime.RawExtension, resource metav1.GroupVersionResource) (*flowv1alpha1.JobFlow, error) {
	jobFlowResource := metav1.GroupVersionResource{Group: flowv1alpha1.SchemeGroupVersion.Group, Version: flowv1alpha1.SchemeGroupVersion.Version, Resource: "jobflows"}
	raw := object.Raw
	jobFlow := flowv1alpha1.JobFlow{}

	if resource != jobFlowResource {
		klog.Errorf("expect resource to be %s", jobFlowResource)
		return &jobFlow, fmt.Errorf("expect resource to be %s", jobFlowResource)
	}

	deserializer := Codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(raw, nil, &jobFlow); err != nil {
		return &jobFlow, err
	}
	klog.V(3).Infof("decoded jobflow uid=%s namespace=%s name=%s", jobFlow.UID, jobFlow.Namespace, jobFlow.Name)
	klog.V(7).Infof("the jobflow struct is %+v", jobFlow)

	return &jobFlow, nil
}
