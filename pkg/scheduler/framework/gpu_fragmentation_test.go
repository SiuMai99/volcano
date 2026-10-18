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

package framework

import (
	"errors"
	"testing"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
)

func TestGPUFragmentationPredicateRequiresEveryEnabledPlugin(t *testing.T) {
	yes := true
	ssn := &Session{
		Tiers: []conf.Tier{{Plugins: []conf.PluginOption{
			{Name: "supported", EnabledPredicate: &yes},
			{Name: "unsupported", EnabledPredicate: &yes},
		}}},
		predicateFns:                 map[string]api.PredicateFn{},
		gpuFragmentationPredicateFns: map[string]api.PredicateFn{},
	}
	ssn.AddPredicateFn("supported", func(*api.TaskInfo, *api.NodeInfo) error { return nil })
	ssn.AddGPUFragmentationPredicateFn("supported", func(*api.TaskInfo, *api.NodeInfo) error { return nil })
	ssn.AddPredicateFn("unsupported", func(*api.TaskInfo, *api.NodeInfo) error { return nil })
	if ssn.SupportsGPUFragmentationDiagnosis() {
		t.Fatal("an unregistered diagnostic predicate must block diagnosis")
	}
	ssn.AddGPUFragmentationPredicateFn("unsupported", func(*api.TaskInfo, *api.NodeInfo) error {
		return errors.New("unexpected plugin failure")
	})
	if !ssn.SupportsGPUFragmentationDiagnosis() {
		t.Fatal("all enabled predicates have diagnostic counterparts")
	}
	if _, known := ssn.GPUFragmentationPredicate(&api.TaskInfo{}, &api.NodeInfo{}); known {
		t.Fatal("unexpected plugin failure must not be treated as an ineligible node")
	}
	ssn.AddGPUFragmentationPredicateFn("unsupported", func(*api.TaskInfo, *api.NodeInfo) error {
		return api.NewFitErrWithStatus(&api.TaskInfo{}, &api.NodeInfo{}, &api.Status{Code: api.Unschedulable, Reason: "selector mismatch"})
	})
	if fits, known := ssn.GPUFragmentationPredicate(&api.TaskInfo{}, &api.NodeInfo{}); !known || fits {
		t.Fatalf("expected a proven ineligible node, got fits=%v known=%v", fits, known)
	}
}
