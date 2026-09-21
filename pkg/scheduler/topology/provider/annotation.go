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

package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	// AnnotationKey is the provisional Alpha Node annotation managed by a
	// trusted topology publisher. Workload and scheduler identities must not
	// receive permission to write it.
	AnnotationKey = "volcano.sh/xpu-topology"
	// MaxAnnotationBytes bounds strict decoding before allocating the payload.
	MaxAnnotationBytes = 1 << 20
)

// AnnotationDocument is the exact strict JSON payload for AnnotationProvider.
// Node identity is deliberately absent: the provider derives it from the Node
// object observed by the scheduler.
type AnnotationDocument struct {
	ResourceName     corev1.ResourceName `json:"resourceName"`
	SourceGeneration uint64              `json:"sourceGeneration"`
	Devices          []DeviceFact        `json:"devices"`
	LocalDomains     []LocalDomainFact   `json:"localDomains"`
	Fabrics          []FabricFact        `json:"fabrics,omitempty"`
}

// ParseAnnotation strictly decodes one Node annotation into a ReplaceFacts
// update. It rejects unknown fields, duplicate object keys, trailing data, and
// over-sized payloads before the Normalizer examines topology semantics.
func ParseAnnotation(identity ProviderIdentityRef, node api.NodeObservation, raw string, observedAt, freshUntil time.Time) (ProviderNodeUpdate, error) {
	if len(raw) > MaxAnnotationBytes {
		return ProviderNodeUpdate{}, fmt.Errorf("decode annotation %q: exceeds %d byte limit", AnnotationKey, MaxAnnotationBytes)
	}
	if len(bytes.TrimSpace([]byte(raw))) == 0 {
		return ProviderNodeUpdate{}, fmt.Errorf("decode annotation %q: empty input", AnnotationKey)
	}
	data := []byte(raw)
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return ProviderNodeUpdate{}, fmt.Errorf("decode annotation %q: %w", AnnotationKey, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document AnnotationDocument
	if err := decoder.Decode(&document); err != nil {
		return ProviderNodeUpdate{}, fmt.Errorf("decode annotation %q: %w", AnnotationKey, err)
	}
	if err := ensureEOF(decoder); err != nil {
		return ProviderNodeUpdate{}, fmt.Errorf("decode annotation %q: %w", AnnotationKey, err)
	}
	if document.SourceGeneration == 0 {
		return ProviderNodeUpdate{}, fmt.Errorf("decode annotation %q: sourceGeneration must be greater than zero", AnnotationKey)
	}
	return ProviderNodeUpdate{
		ProviderID:          identity.ProviderID,
		IdentityNamespace:   identity.Namespace,
		ResourceName:        document.ResourceName,
		NodeName:            node.Identity.Name,
		NodeUID:             node.Identity.UID,
		NodeResourceVersion: node.ResourceVersion,
		SourceGeneration:    document.SourceGeneration,
		ObservedAt:          observedAt,
		FreshUntil:          freshUntil,
		Operation:           ReplaceFacts,
		Facts: &NodeTopologyFacts{
			ResourceName: document.ResourceName,
			Devices:      document.Devices,
			LocalDomains: document.LocalDomains,
			Fabrics:      document.Fabrics,
		},
	}, nil
}

// ClearAnnotation records a valid source observation that this provider has no
// inventory on the current Node. It is used only after the source has checked
// for absence; a parse error must never be converted into ClearFacts.
func ClearAnnotation(identity ProviderIdentityRef, resourceName corev1.ResourceName, node api.NodeObservation, sourceGeneration uint64, observedAt, freshUntil time.Time) ProviderNodeUpdate {
	return ProviderNodeUpdate{
		ProviderID:          identity.ProviderID,
		IdentityNamespace:   identity.Namespace,
		ResourceName:        resourceName,
		NodeName:            node.Identity.Name,
		NodeUID:             node.Identity.UID,
		NodeResourceVersion: node.ResourceVersion,
		SourceGeneration:    sourceGeneration,
		ObservedAt:          observedAt,
		FreshUntil:          freshUntil,
		Operation:           ClearFacts,
	}
}

// MockProvider is a fixture-only source. It performs no discovery and must not
// be wired into production scheduler startup or used as exact-ID evidence.
type MockProvider struct {
	Identity ProviderIdentityRef
}

// Replace constructs a fixture ReplaceFacts update from caller-owned facts.
func (m MockProvider) Replace(node api.NodeObservation, sourceGeneration uint64, facts NodeTopologyFacts, observedAt, freshUntil time.Time) ProviderNodeUpdate {
	return ProviderNodeUpdate{
		ProviderID:          m.Identity.ProviderID,
		IdentityNamespace:   m.Identity.Namespace,
		ResourceName:        facts.ResourceName,
		NodeName:            node.Identity.Name,
		NodeUID:             node.Identity.UID,
		NodeResourceVersion: node.ResourceVersion,
		SourceGeneration:    sourceGeneration,
		ObservedAt:          observedAt,
		FreshUntil:          freshUntil,
		Operation:           ReplaceFacts,
		Facts:               &facts,
	}
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

func ensureEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("contains trailing JSON value")
}

// AnnotationPayload reports whether a Node annotation map contains a value for
// the managed key without converting a missing key into a clear observation.
func AnnotationPayload(annotations map[string]string) (string, bool) {
	if annotations == nil {
		return "", false
	}
	value, found := annotations[AnnotationKey]
	return strings.TrimSpace(value), found
}
