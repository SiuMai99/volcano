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
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestTrackerForgetNodeObservationRetiresWholeNodeIncarnation(t *testing.T) {
	tracker := NewTracker()
	oldUpdate := ProviderNodeUpdate{
		ProviderID:          "test-provider",
		IdentityNamespace:   "test.example",
		ResourceName:        "nvidia.com/gpu",
		NodeName:            "node-a",
		NodeUID:             types.UID("uid-a"),
		NodeResourceVersion: "1",
		SourceGeneration:    1,
		Operation:           ClearFacts,
	}
	if _, err := tracker.Apply(oldUpdate, "same-content", nil); err != nil {
		t.Fatalf("Apply(old) error = %v", err)
	}
	newUpdate := oldUpdate
	newUpdate.NodeResourceVersion = "2"
	if _, err := tracker.Apply(newUpdate, "same-content", nil); err != nil {
		t.Fatalf("Apply(new) error = %v", err)
	}

	otherNode := oldUpdate
	otherNode.NodeUID = "uid-b"
	if _, err := tracker.Apply(otherNode, "other-content", nil); err != nil {
		t.Fatalf("Apply(other Node) error = %v", err)
	}

	if removed := tracker.ForgetNodeObservation(api.NodeIdentity{Name: "node-a", UID: "uid-a"}); !removed {
		t.Fatal("ForgetNodeObservation() did not retire the Node incarnation")
	}
	if _, found := tracker.Record(newUpdate.Key()); found {
		t.Fatal("retired Node incarnation still has a provider record")
	}
	if record, found := tracker.Record(otherNode.Key()); !found || record.Update.NodeUID != otherNode.NodeUID {
		t.Fatalf("unrelated Node record was retired: %#v, found=%t", record, found)
	}
}
