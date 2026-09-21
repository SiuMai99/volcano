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
	"errors"
	"strings"
	"testing"
	"time"

	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestParseAnnotationStrict(t *testing.T) {
	node := api.NodeObservation{
		Identity:        api.NodeIdentity{Name: "node-a", UID: "uid-a"},
		ResourceVersion: "42",
	}
	identity := ProviderIdentityRef{ProviderID: "annotation-v1", Namespace: "example.test"}
	valid := `{
  "resourceName":"nvidia.com/gpu",
  "sourceGeneration":7,
  "devices":[{"id":"GPU-a","health":"Healthy"}],
  "localDomains":[{"id":"nvlink-0","domainClass":"local-scale-up","deviceIDs":["GPU-a"]}]
}`
	update, err := ParseAnnotation(identity, node, valid, time.Unix(10, 0), time.Unix(20, 0))
	if err != nil {
		t.Fatalf("ParseAnnotation() error = %v", err)
	}
	if update.Operation != ReplaceFacts || update.SourceGeneration != 7 || update.Facts == nil {
		t.Fatalf("ParseAnnotation() = %#v, want replace generation 7", update)
	}
	if update.NodeName != node.Identity.Name || update.NodeUID != node.Identity.UID || update.NodeResourceVersion != node.ResourceVersion {
		t.Fatalf("ParseAnnotation() did not derive Node identity: %#v", update)
	}

	for name, raw := range map[string]string{
		"unknown field":      `{"resourceName":"nvidia.com/gpu","sourceGeneration":1,"devices":[],"localDomains":[],"unknown":true}`,
		"duplicate key":      `{"resourceName":"nvidia.com/gpu","resourceName":"nvidia.com/gpu","sourceGeneration":1,"devices":[],"localDomains":[]}`,
		"trailing value":     valid + ` {}`,
		"zero generation":    `{"resourceName":"nvidia.com/gpu","sourceGeneration":0,"devices":[],"localDomains":[]}`,
		"malformed document": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseAnnotation(identity, node, raw, time.Time{}, time.Time{}); err == nil {
				t.Fatalf("ParseAnnotation(%q) unexpectedly succeeded", raw)
			}
		})
	}
}

func TestTrackerGenerationAndFailureAtomicity(t *testing.T) {
	tracker := NewTracker()
	update := trackerTestUpdate(1)
	if state := tracker.State(update.Key()); state != ProviderNodePending {
		t.Fatalf("initial State() = %q, want Pending", state)
	}
	record, err := tracker.Apply(update, "canonical-one", nil)
	if err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if record.State != ProviderNodeSynced {
		t.Fatalf("state = %q, want Synced", record.State)
	}
	if state := tracker.State(update.Key()); state != ProviderNodeSynced {
		t.Fatalf("State() after valid replace = %q, want Synced", state)
	}

	// The tracker must own a deep copy rather than retain caller facts.
	update.Facts.Devices[0].ID = "mutated-after-apply"
	stored, found := tracker.Record(record.Key)
	if !found || stored.Update.Facts.Devices[0].ID != "GPU-a" {
		t.Fatalf("Record() = %#v, want immutable copy", stored)
	}

	heartbeat := trackerTestUpdate(1)
	heartbeat.NodeResourceVersion = "new-opaque-version"
	heartbeat.FreshUntil = time.Unix(100, 0)
	if _, err := tracker.Apply(heartbeat, "canonical-one", nil); err != nil {
		t.Fatalf("same-content heartbeat Apply() error = %v", err)
	}
	if _, err := tracker.Apply(trackerTestUpdate(1), "different-content", nil); err == nil {
		t.Fatal("same generation with changed content unexpectedly succeeded")
	}
	if _, err := tracker.Apply(trackerTestUpdate(0), "older", nil); err == nil {
		t.Fatal("older generation unexpectedly succeeded")
	}

	rejected := trackerTestUpdate(2)
	if _, err := tracker.Apply(rejected, "canonical-two", func([]ProviderNodeRecord) error {
		return errors.New("fabric conflict")
	}); err == nil || !strings.Contains(err.Error(), "fabric conflict") {
		t.Fatalf("validator failure = %v, want fabric conflict", err)
	}
	stored, _ = tracker.Record(record.Key)
	if stored.Update.SourceGeneration != 1 || stored.ContentFingerprint != "canonical-one" {
		t.Fatalf("failed update replaced valid record: %#v", stored)
	}

	clear := trackerTestUpdate(2)
	clear.Operation = ClearFacts
	clear.Facts = nil
	cleared, err := tracker.Apply(clear, "clear-fingerprint", nil)
	if err != nil {
		t.Fatalf("ClearFacts Apply() error = %v", err)
	}
	if cleared.State != ProviderNodeSynced || cleared.Update.Facts != nil {
		t.Fatalf("clear record = %#v, want synced empty facts", cleared)
	}
}

func trackerTestUpdate(generation uint64) ProviderNodeUpdate {
	return ProviderNodeUpdate{
		ProviderID:          "annotation-v1",
		IdentityNamespace:   "example.test",
		ResourceName:        "nvidia.com/gpu",
		NodeName:            "node-a",
		NodeUID:             "uid-a",
		NodeResourceVersion: "1",
		SourceGeneration:    generation,
		Operation:           ReplaceFacts,
		Facts: &NodeTopologyFacts{
			ResourceName: "nvidia.com/gpu",
			Devices:      []DeviceFact{{ID: "GPU-a", Health: "Healthy"}},
		},
	}
}
