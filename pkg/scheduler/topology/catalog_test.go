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
	"errors"
	"os"
	"strings"
	"testing"

	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

const validCatalog = `{
  "resources": [
    {
      "resourceName": "nvidia.com/gpu",
      "description": "NVIDIA whole GPUs",
      "domainClasses": [
        {"scope": "Node", "name": "pcie-root", "description": "PCIe root"},
        {"scope": "Fabric", "name": "scale-up-fabric", "description": "explicit fabric"},
        {"scope": "Node", "name": "local-scale-up", "description": "NVLink island"}
      ]
    },
    {
      "resourceName": "huawei.com/ascend910",
      "domainClasses": [
        {"scope": "Node", "name": "local-scale-up", "description": "HCCS domain"}
      ]
    }
  ]
}`

func TestLoadCatalogStrictAndStable(t *testing.T) {
	catalog, err := LoadCatalog(strings.NewReader(validCatalog))
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}

	nvidiaNode := api.DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}
	huaweiNode := api.DomainClassKey{ResourceName: "huawei.com/ascend910", Scope: scheduling.DeviceTopologyDomainScopeNode, Name: "local-scale-up"}
	if !catalog.HasDomainClass(nvidiaNode) || !catalog.HasDomainClass(huaweiNode) {
		t.Fatalf("catalog did not preserve resource-qualified class identities")
	}
	if descriptor, found := catalog.Lookup(nvidiaNode); !found || descriptor.Description != "NVLink island" {
		t.Fatalf("Lookup() = %#v, %v; want NVIDIA class descriptor", descriptor, found)
	}

	descriptors := catalog.Descriptors()
	if len(descriptors) != 2 || descriptors[0].ResourceName != "huawei.com/ascend910" {
		t.Fatalf("Descriptors() order = %#v, want resource-name order", descriptors)
	}
	if got, want := descriptors[1].Classes[0].Key.Scope, scheduling.DeviceTopologyDomainScopeFabric; got != want {
		t.Fatalf("class order = %q, want %q", got, want)
	}

	// Callers must not be able to mutate the loaded catalog by changing the
	// copied descriptor slice.
	descriptors[1].Classes[0].Description = "mutated"
	if descriptor, _ := catalog.Lookup(api.DomainClassKey{ResourceName: "nvidia.com/gpu", Scope: scheduling.DeviceTopologyDomainScopeFabric, Name: "scale-up-fabric"}); descriptor.Description != "explicit fabric" {
		t.Fatalf("catalog mutated through Descriptors(): %#v", descriptor)
	}
}

func TestLoadCatalogRejectsStrictAndInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name    string
		catalog string
	}{
		{name: "unknown field", catalog: `{"resources":[],"unexpected":true}`},
		{name: "duplicate JSON key", catalog: `{"resources":[],"resources":[]}`},
		{name: "trailing JSON", catalog: validCatalog + ` {}`},
		{name: "empty", catalog: ``},
		{name: "native resource", catalog: `{"resources":[{"resourceName":"cpu","domainClasses":[{"scope":"Node","name":"local-scale-up"}]}]}`},
		{name: "duplicate resource", catalog: `{"resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Node","name":"a"}]},{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Fabric","name":"b"}]}]}`},
		{name: "duplicate class", catalog: `{"resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Node","name":"a"},{"scope":"Node","name":"a"}]}]}`},
		{name: "invalid scope", catalog: `{"resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Rack","name":"a"}]}]}`},
		{name: "invalid name", catalog: `{"resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[{"scope":"Node","name":"Upper"}]}]}`},
		{name: "empty classes", catalog: `{"resources":[{"resourceName":"nvidia.com/gpu","domainClasses":[]}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadCatalog(strings.NewReader(tt.catalog))
			if err == nil {
				t.Fatal("LoadCatalog() unexpectedly accepted invalid catalog")
			}
		})
	}
}

func TestLoadCatalogFile(t *testing.T) {
	path := t.TempDir() + "/catalog.json"
	if err := os.WriteFile(path, []byte(validCatalog), 0o600); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
	if _, err := LoadCatalogFile(path); err != nil {
		t.Fatalf("LoadCatalogFile() error = %v", err)
	}
	if _, err := LoadCatalogFile(path + ".missing"); err == nil {
		t.Fatal("LoadCatalogFile() unexpectedly accepted a missing path")
	}
}

func TestCatalogValidationError(t *testing.T) {
	_, err := LoadCatalog(strings.NewReader(`{"resources":[]}`))
	var validationErr *CatalogValidationError
	if !errors.As(err, &validationErr) || len(validationErr.Problems) == 0 {
		t.Fatalf("LoadCatalog() error = %v, want CatalogValidationError", err)
	}
}
