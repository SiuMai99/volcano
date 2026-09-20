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

package xputopologyaware

import (
	"testing"

	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func TestParseArguments(t *testing.T) {
	tests := []struct {
		name         string
		arguments    framework.Arguments
		wantValid    bool
		wantProvider string
		wantCatalog  bool
	}{
		{name: "missing required provider", arguments: framework.Arguments{}, wantValid: false},
		{
			name: "annotation provider",
			arguments: framework.Arguments{
				ProviderArgument:    AnnotationProvider,
				CatalogPathArgument: "/etc/volcano/xpu-catalog.json",
			},
			wantValid:    true,
			wantProvider: AnnotationProvider,
			wantCatalog:  true,
		},
		{name: "unknown provider", arguments: framework.Arguments{ProviderArgument: "nvidia"}, wantValid: false, wantProvider: "nvidia"},
		{name: "wrong provider type", arguments: framework.Arguments{ProviderArgument: 1}, wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := ParseArguments(tt.arguments)
			if got := config.Valid(); got != tt.wantValid {
				t.Fatalf("Valid() = %v, want %v", got, tt.wantValid)
			}
			if got := config.Provider; got != tt.wantProvider {
				t.Fatalf("Provider = %q, want %q", got, tt.wantProvider)
			}
			if got := config.CatalogPresent; got != tt.wantCatalog {
				t.Fatalf("CatalogPresent = %v, want %v", got, tt.wantCatalog)
			}
		})
	}
}

func TestConfigIdentityIsOrderIndependent(t *testing.T) {
	first := []conf.PluginOption{
		{Name: PluginName, Arguments: map[string]interface{}{ProviderArgument: AnnotationProvider, CatalogPathArgument: "/a"}},
		{Name: PluginName, Arguments: map[string]interface{}{ProviderArgument: MockProvider, CatalogPathArgument: "/b"}},
	}
	second := []conf.PluginOption{first[1], first[0]}
	if got, want := ConfigIdentity(first), ConfigIdentity(second); got != want {
		t.Fatalf("ConfigIdentity() = %q, want %q", got, want)
	}
}

func TestPluginContract(t *testing.T) {
	plugin := New(framework.Arguments{
		ProviderArgument: AnnotationProvider,
	})
	if got := plugin.Name(); got != PluginName {
		t.Fatalf("Name() = %q, want %q", got, PluginName)
	}
	// The skeleton's lifecycle methods are intentionally no-ops: they must not
	// require a live Session or start a process-scoped manager.
	plugin.OnSessionOpen(nil)
	plugin.OnSessionClose(nil)
}
