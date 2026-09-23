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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// XPUAssignmentAnnotationKey stores the scheduler-owned exact device
	// assignment on a Pod. A value on an unbound Pod is not recovery evidence.
	XPUAssignmentAnnotationKey = "volcano.sh/xpu-assignment"

	// XPUAssignmentVersion is the only assignment envelope version accepted by
	// the Pod-derived Topology Alpha contract.
	XPUAssignmentVersion = 1
)

// XPUContainerKind identifies which Pod container list owns a device request.
// Restartable init containers remain distinct because they overlap regular
// containers at runtime, even though both kinds live in PodSpec.InitContainers.
type XPUContainerKind string

const (
	XPUContainerRegular         XPUContainerKind = "regular"
	XPUContainerInit            XPUContainerKind = "init"
	XPUContainerRestartableInit XPUContainerKind = "restartable-init"
)

// XPUContainerRef is the stable, minimum identity of one container inside a
// Pod. Pod UID comes from the API object and is deliberately not duplicated.
type XPUContainerRef struct {
	Kind XPUContainerKind `json:"kind"`
	Name string           `json:"name"`
}

// XPUResourceRequest preserves the unique consumer and whole-device count for
// one target extended resource.
type XPUResourceRequest struct {
	Container    XPUContainerRef
	ResourceName corev1.ResourceName
	Count        int64
}

// XPUAssignmentEntry records the Provider and exact DeviceKeys selected for
// one resource consumer. Domain, Fabric, Group and plan summaries are derived
// from the current policy and topology snapshot and are not persisted here.
type XPUAssignmentEntry struct {
	Container    XPUContainerRef     `json:"container"`
	ResourceName corev1.ResourceName `json:"resourceName"`
	Provider     string              `json:"provider"`
	DeviceKeys   []string            `json:"deviceKeys"`
}

// XPUAssignment is the Pod-level canonical assignment envelope. A Pod may
// consume multiple target resources, but each resource has exactly one entry.
type XPUAssignment struct {
	Version     int                  `json:"version"`
	Assignments []XPUAssignmentEntry `json:"assignments"`
}

// CanonicalXPUDeviceKey returns the wire identity used by the Alpha assignment
// annotation. The source device ID must already be Provider-normalized.
func CanonicalXPUDeviceKey(nodeUID types.UID, deviceID SourceDeviceID) (string, error) {
	owner := strings.TrimSpace(string(nodeUID))
	value := strings.TrimSpace(string(deviceID))
	if owner == "" {
		return "", fmt.Errorf("node UID is empty")
	}
	if value == "" {
		return "", fmt.Errorf("device ID is empty")
	}
	if strings.ContainsAny(owner, "/\n\r") {
		return "", fmt.Errorf("node UID contains a key separator")
	}
	if strings.ContainsAny(value, "/\n\r") {
		return "", fmt.Errorf("device ID contains a key separator")
	}
	return owner + "/" + value, nil
}

// ParseXPUDeviceKey expands one assignment wire key using the trusted Provider
// identity that resolved its enclosing entry. Provider namespace is supplied
// separately because it is not necessarily the resource-name prefix.
func ParseXPUDeviceKey(resourceName corev1.ResourceName, providerID, providerNamespace, raw string) (DeviceKey, error) {
	if !isXPUAssignmentResourceName(resourceName) {
		return DeviceKey{}, fmt.Errorf("resource name %q must be a qualified extended resource name", resourceName)
	}
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return DeviceKey{}, fmt.Errorf("provider is empty")
	}
	providerNamespace = strings.TrimSpace(providerNamespace)
	if providerNamespace == "" {
		return DeviceKey{}, fmt.Errorf("provider namespace is empty")
	}
	owner, value, err := parseXPUAssignmentWireDeviceKey(raw)
	if err != nil {
		return DeviceKey{}, err
	}
	return DeviceKey{
		ResourceName: resourceName,
		OwnerNodeUID: owner,
		ID: DeviceID{
			ProviderID: providerID,
			Namespace:  providerNamespace,
			Value:      value,
		},
	}, nil
}

// CanonicalizeXPUAssignment validates and stable-sorts one assignment without
// mutating the caller-owned slices.
func CanonicalizeXPUAssignment(assignment XPUAssignment) (XPUAssignment, error) {
	if assignment.Version != XPUAssignmentVersion {
		return XPUAssignment{}, fmt.Errorf("unsupported assignment version %d", assignment.Version)
	}
	if len(assignment.Assignments) == 0 {
		return XPUAssignment{}, fmt.Errorf("assignments is empty")
	}

	canonical := XPUAssignment{Version: assignment.Version, Assignments: make([]XPUAssignmentEntry, len(assignment.Assignments))}
	seenResources := make(map[corev1.ResourceName]struct{}, len(assignment.Assignments))
	for index := range assignment.Assignments {
		entry := assignment.Assignments[index]
		if err := validateXPUContainerRef(entry.Container); err != nil {
			return XPUAssignment{}, fmt.Errorf("assignment[%d]: %w", index, err)
		}
		if !isXPUAssignmentResourceName(entry.ResourceName) {
			return XPUAssignment{}, fmt.Errorf("assignment[%d]: resource name %q must be a qualified extended resource name", index, entry.ResourceName)
		}
		if _, found := seenResources[entry.ResourceName]; found {
			return XPUAssignment{}, fmt.Errorf("assignment[%d]: resource name %q is duplicated", index, entry.ResourceName)
		}
		seenResources[entry.ResourceName] = struct{}{}
		entry.Provider = strings.TrimSpace(entry.Provider)
		if entry.Provider == "" {
			return XPUAssignment{}, fmt.Errorf("assignment[%d]: provider is empty", index)
		}
		if len(entry.DeviceKeys) == 0 {
			return XPUAssignment{}, fmt.Errorf("assignment[%d]: deviceKeys is empty", index)
		}

		entry.DeviceKeys = append([]string(nil), entry.DeviceKeys...)
		seenKeys := make(map[string]struct{}, len(entry.DeviceKeys))
		for keyIndex, raw := range entry.DeviceKeys {
			owner, value, err := parseXPUAssignmentWireDeviceKey(raw)
			if err != nil {
				return XPUAssignment{}, fmt.Errorf("assignment[%d].deviceKeys[%d]: %w", index, keyIndex, err)
			}
			canonicalKey, err := CanonicalXPUDeviceKey(owner, value)
			if err != nil {
				return XPUAssignment{}, fmt.Errorf("assignment[%d].deviceKeys[%d]: %w", index, keyIndex, err)
			}
			if _, found := seenKeys[canonicalKey]; found {
				return XPUAssignment{}, fmt.Errorf("assignment[%d]: device key %q is duplicated", index, canonicalKey)
			}
			seenKeys[canonicalKey] = struct{}{}
			entry.DeviceKeys[keyIndex] = canonicalKey
		}
		sort.Strings(entry.DeviceKeys)
		canonical.Assignments[index] = entry
	}

	sort.Slice(canonical.Assignments, func(i, j int) bool { return xpuAssignmentEntryLess(canonical.Assignments[i], canonical.Assignments[j]) })
	return canonical, nil
}

// MarshalXPUAssignment validates and emits the unique compact JSON form used
// in Pod metadata.
func MarshalXPUAssignment(assignment XPUAssignment) ([]byte, error) {
	canonical, err := CanonicalizeXPUAssignment(assignment)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("encode assignment: %w", err)
	}
	return data, nil
}

// ParseXPUAssignment strictly decodes and canonicalizes one Pod annotation.
// Unknown fields, duplicate JSON object keys and trailing values are rejected.
func ParseXPUAssignment(data []byte) (XPUAssignment, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return XPUAssignment{}, fmt.Errorf("decode assignment: empty input")
	}
	if err := rejectDuplicateXPUAssignmentJSONKeys(data); err != nil {
		return XPUAssignment{}, fmt.Errorf("decode assignment: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var assignment XPUAssignment
	if err := decoder.Decode(&assignment); err != nil {
		return XPUAssignment{}, fmt.Errorf("decode assignment: %w", err)
	}
	if err := ensureXPUAssignmentEOF(decoder); err != nil {
		return XPUAssignment{}, fmt.Errorf("decode assignment: %w", err)
	}
	return CanonicalizeXPUAssignment(assignment)
}

// XPUResourceRequestForPod returns the unique whole-device consumer for one
// resource across regular, init and restartable init containers.
func XPUResourceRequestForPod(pod *corev1.Pod, resourceName corev1.ResourceName) (XPUResourceRequest, error) {
	if pod == nil {
		return XPUResourceRequest{}, fmt.Errorf("Pod is required")
	}
	if !isXPUAssignmentResourceName(resourceName) {
		return XPUResourceRequest{}, fmt.Errorf("resource name %q must be a qualified extended resource name", resourceName)
	}

	var result *XPUResourceRequest
	checkContainer := func(container corev1.Container, kind XPUContainerKind) error {
		request, requested := container.Resources.Requests[resourceName]
		limit, limited := container.Resources.Limits[resourceName]
		if !requested && !limited {
			return nil
		}
		ref := XPUContainerRef{Kind: kind, Name: container.Name}
		if result != nil {
			return fmt.Errorf("only one container may request device resource %s; already found in %s %q, also found in %s %q",
				resourceName, result.Container.Kind, result.Container.Name, ref.Kind, ref.Name)
		}
		if !limited {
			return fmt.Errorf("%s %q: device resource limit must be set", kind, container.Name)
		}
		if limit.Sign() <= 0 || limit.MilliValue()%1000 != 0 {
			return fmt.Errorf("%s %q: limit must be a positive integral device count", kind, container.Name)
		}
		if requested && request.Cmp(limit) != 0 {
			return fmt.Errorf("%s %q: request must equal limit when both are set", kind, container.Name)
		}
		value := XPUResourceRequest{Container: ref, ResourceName: resourceName, Count: limit.Value()}
		result = &value
		return nil
	}

	for _, container := range pod.Spec.Containers {
		if err := checkContainer(container, XPUContainerRegular); err != nil {
			return XPUResourceRequest{}, err
		}
	}
	for _, container := range pod.Spec.InitContainers {
		kind := XPUContainerInit
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			kind = XPUContainerRestartableInit
		}
		if err := checkContainer(container, kind); err != nil {
			return XPUResourceRequest{}, err
		}
	}
	if result == nil {
		return XPUResourceRequest{}, fmt.Errorf("device resource %s must be requested by exactly one container", resourceName)
	}
	return *result, nil
}

// ValidateXPUAssignmentForPod binds the persisted envelope to the actual Pod
// request shape. It does not make an unbound Pod a recovery authority.
func ValidateXPUAssignmentForPod(pod *corev1.Pod, assignment XPUAssignment) error {
	canonical, err := CanonicalizeXPUAssignment(assignment)
	if err != nil {
		return err
	}
	for index, entry := range canonical.Assignments {
		request, err := XPUResourceRequestForPod(pod, entry.ResourceName)
		if err != nil {
			return fmt.Errorf("assignment[%d]: %w", index, err)
		}
		if request.Container != entry.Container {
			return fmt.Errorf("assignment[%d]: container %s %q does not match Pod consumer %s %q",
				index, entry.Container.Kind, entry.Container.Name, request.Container.Kind, request.Container.Name)
		}
		if int64(len(entry.DeviceKeys)) != request.Count {
			return fmt.Errorf("assignment[%d]: device key count %d does not match limit %d", index, len(entry.DeviceKeys), request.Count)
		}
	}
	return nil
}

// ValidateXPUAssignmentNodeUID rejects an assignment recovered from a replaced
// same-name Node incarnation.
func ValidateXPUAssignmentNodeUID(assignment XPUAssignment, nodeUID types.UID) error {
	canonical, err := CanonicalizeXPUAssignment(assignment)
	if err != nil {
		return err
	}
	for index, entry := range canonical.Assignments {
		for keyIndex, raw := range entry.DeviceKeys {
			owner, _, err := parseXPUAssignmentWireDeviceKey(raw)
			if err != nil {
				return fmt.Errorf("assignment[%d].deviceKeys[%d]: %w", index, keyIndex, err)
			}
			if owner != nodeUID {
				return fmt.Errorf("assignment[%d].deviceKeys[%d]: DeviceKey belongs to NodeUID %q, want %q", index, keyIndex, owner, nodeUID)
			}
		}
	}
	return nil
}

func validateXPUContainerRef(ref XPUContainerRef) error {
	switch ref.Kind {
	case XPUContainerRegular, XPUContainerInit, XPUContainerRestartableInit:
	default:
		return fmt.Errorf("container kind %q is unsupported", ref.Kind)
	}
	if problems := validation.IsDNS1123Label(ref.Name); len(problems) != 0 {
		return fmt.Errorf("container name %q is invalid: %s", ref.Name, strings.Join(problems, ", "))
	}
	return nil
}

func isXPUAssignmentResourceName(resourceName corev1.ResourceName) bool {
	name := string(resourceName)
	return strings.Contains(name, "/") && len(validation.IsQualifiedName(name)) == 0
}

func xpuAssignmentEntryLess(left, right XPUAssignmentEntry) bool {
	if left.ResourceName != right.ResourceName {
		return left.ResourceName < right.ResourceName
	}
	if left.Provider != right.Provider {
		return left.Provider < right.Provider
	}
	if left.Container.Kind != right.Container.Kind {
		return left.Container.Kind < right.Container.Kind
	}
	if left.Container.Name != right.Container.Name {
		return left.Container.Name < right.Container.Name
	}
	return slices.Compare(left.DeviceKeys, right.DeviceKeys) < 0
}

func parseXPUAssignmentWireDeviceKey(raw string) (types.UID, SourceDeviceID, error) {
	original := raw
	raw = strings.TrimSpace(raw)
	parts := strings.SplitN(raw, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid canonical DeviceKey %q", original)
	}
	owner := types.UID(parts[0])
	value := SourceDeviceID(parts[1])
	canonical, err := CanonicalXPUDeviceKey(owner, value)
	if err != nil {
		return "", "", fmt.Errorf("invalid canonical DeviceKey %q: %w", original, err)
	}
	if canonical != original {
		return "", "", fmt.Errorf("DeviceKey %q is not canonical; want %q", original, canonical)
	}
	return owner, value, nil
}

func rejectDuplicateXPUAssignmentJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanXPUAssignmentJSONValue(decoder); err != nil {
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

func scanXPUAssignmentJSONValue(decoder *json.Decoder) error {
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
			if err := scanXPUAssignmentJSONValue(decoder); err != nil {
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
			if err := scanXPUAssignmentJSONValue(decoder); err != nil {
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

func ensureXPUAssignmentEOF(decoder *json.Decoder) error {
	var trailing interface{}
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("contains trailing JSON value")
}
