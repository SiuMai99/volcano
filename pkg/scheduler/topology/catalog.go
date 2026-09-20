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

package topology

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// MaxCatalogBytes bounds the static catalog read before decoding. It is a
// parser safety limit, not a catalog versioning or rollout mechanism.
const MaxCatalogBytes = 1 << 20

// CatalogDocument is the single strict JSON input accepted by the Alpha
// scheduler catalog. It is delivered locally to the scheduler process and is
// never watched or replaced while that process is running.
type CatalogDocument struct {
	Resources []CatalogResourceDescriptor `json:"resources"`
}

// CatalogResourceDescriptor declares all fixed topology classes for one
// extended resource.
type CatalogResourceDescriptor struct {
	ResourceName  corev1.ResourceName      `json:"resourceName"`
	Description   string                   `json:"description,omitempty"`
	DomainClasses []CatalogClassDescriptor `json:"domainClasses"`
}

// CatalogClassDescriptor is one fixed, administrator-defined class.
type CatalogClassDescriptor struct {
	Scope       scheduling.DeviceTopologyDomainScope `json:"scope"`
	Name        scheduling.DeviceTopologyDomainClass `json:"name"`
	Description string                               `json:"description,omitempty"`
}

// CatalogValidationError reports all semantic problems found after strict JSON
// decoding. Any one problem makes the entire catalog unavailable.
type CatalogValidationError struct {
	Problems []string
}

func (e *CatalogValidationError) Error() string {
	return "invalid xpu topology catalog: " + strings.Join(e.Problems, "; ")
}

// Catalog is an immutable in-memory index of a successfully loaded static
// catalog. All fields are private; callers receive copies of descriptors.
type Catalog struct {
	descriptors []api.ResourceTopologyDescriptor
	classes     map[api.DomainClassKey]api.DomainClassDescriptor
}

var _ api.CatalogView = (*Catalog)(nil)

// LoadCatalog reads one strict JSON catalog and returns a fixed immutable
// lookup. Unknown fields, duplicate JSON object keys, trailing data, duplicate
// class keys, and invalid resource/scope/name combinations are all failures.
func LoadCatalog(reader io.Reader) (*Catalog, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxCatalogBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read xpu topology catalog: %w", err)
	}
	if len(data) > MaxCatalogBytes {
		return nil, fmt.Errorf("read xpu topology catalog: exceeds %d byte limit", MaxCatalogBytes)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("decode xpu topology catalog: empty input")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return nil, fmt.Errorf("decode xpu topology catalog: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document CatalogDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode xpu topology catalog: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode xpu topology catalog: %w", err)
	}
	return newCatalog(document)
}

// LoadCatalogFile opens and reads one scheduler-local catalog file. Callers
// load it at process activation; this function does not watch the file.
func LoadCatalogFile(path string) (*Catalog, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open xpu topology catalog %q: %w", path, err)
	}
	defer file.Close()
	return LoadCatalog(file)
}

// HasDomainClass implements api.CatalogView.
func (c *Catalog) HasDomainClass(key api.DomainClassKey) bool {
	if c == nil {
		return false
	}
	_, found := c.classes[key]
	return found
}

// Lookup returns a copy of one fixed class descriptor.
func (c *Catalog) Lookup(key api.DomainClassKey) (api.DomainClassDescriptor, bool) {
	if c == nil {
		return api.DomainClassDescriptor{}, false
	}
	descriptor, found := c.classes[key]
	return descriptor, found
}

// Descriptors returns a deep copy in stable resource/class order.
func (c *Catalog) Descriptors() []api.ResourceTopologyDescriptor {
	if c == nil {
		return nil
	}
	descriptors := make([]api.ResourceTopologyDescriptor, len(c.descriptors))
	for i, descriptor := range c.descriptors {
		descriptors[i] = api.ResourceTopologyDescriptor{
			ResourceName: descriptor.ResourceName,
			Description:  descriptor.Description,
			Classes:      append([]api.DomainClassDescriptor(nil), descriptor.Classes...),
		}
	}
	return descriptors
}

func newCatalog(document CatalogDocument) (*Catalog, error) {
	problems := []string{}
	if len(document.Resources) == 0 {
		problems = append(problems, "resources must contain at least one descriptor")
	}

	resourceNames := make(map[corev1.ResourceName]struct{}, len(document.Resources))
	classes := make(map[api.DomainClassKey]api.DomainClassDescriptor)
	descriptors := make([]api.ResourceTopologyDescriptor, 0, len(document.Resources))
	for resourceIndex, resource := range document.Resources {
		prefix := fmt.Sprintf("resources[%d]", resourceIndex)
		if !isExtendedResourceName(resource.ResourceName) {
			problems = append(problems, fmt.Sprintf("%s.resourceName %q must be a qualified extended resource name", prefix, resource.ResourceName))
		}
		if _, exists := resourceNames[resource.ResourceName]; exists {
			problems = append(problems, fmt.Sprintf("%s.resourceName %q is duplicated", prefix, resource.ResourceName))
		}
		resourceNames[resource.ResourceName] = struct{}{}
		if len(resource.DomainClasses) == 0 {
			problems = append(problems, fmt.Sprintf("%s.domainClasses must contain at least one class", prefix))
		}

		descriptor := api.ResourceTopologyDescriptor{
			ResourceName: resource.ResourceName,
			Description:  resource.Description,
			Classes:      make([]api.DomainClassDescriptor, 0, len(resource.DomainClasses)),
		}
		for classIndex, class := range resource.DomainClasses {
			classPrefix := fmt.Sprintf("%s.domainClasses[%d]", prefix, classIndex)
			if class.Scope != scheduling.DeviceTopologyDomainScopeNode && class.Scope != scheduling.DeviceTopologyDomainScopeFabric {
				problems = append(problems, fmt.Sprintf("%s.scope %q must be Node or Fabric", classPrefix, class.Scope))
			}
			if messages := validation.IsDNS1123Label(string(class.Name)); len(messages) > 0 {
				problems = append(problems, fmt.Sprintf("%s.name %q is invalid: %s", classPrefix, class.Name, strings.Join(messages, "; ")))
			}

			key := api.DomainClassKey{ResourceName: resource.ResourceName, Scope: class.Scope, Name: class.Name}
			if _, exists := classes[key]; exists {
				problems = append(problems, fmt.Sprintf("%s duplicates DomainClassKey{%s,%s,%s}", classPrefix, key.ResourceName, key.Scope, key.Name))
			}
			classDescriptor := api.DomainClassDescriptor{Key: key, Description: class.Description}
			classes[key] = classDescriptor
			descriptor.Classes = append(descriptor.Classes, classDescriptor)
		}
		sort.Slice(descriptor.Classes, func(i, j int) bool {
			left, right := descriptor.Classes[i].Key, descriptor.Classes[j].Key
			if left.Scope != right.Scope {
				return left.Scope < right.Scope
			}
			return left.Name < right.Name
		})
		descriptors = append(descriptors, descriptor)
	}
	if len(problems) > 0 {
		return nil, &CatalogValidationError{Problems: problems}
	}
	sort.Slice(descriptors, func(i, j int) bool {
		return descriptors[i].ResourceName < descriptors[j].ResourceName
	})
	return &Catalog{descriptors: descriptors, classes: classes}, nil
}

func isExtendedResourceName(resourceName corev1.ResourceName) bool {
	name := string(resourceName)
	return strings.Contains(name, "/") && len(validation.IsQualifiedName(name)) == 0
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
	var trailing interface{}
	if err := decoder.Decode(&trailing); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("contains trailing JSON value")
}
