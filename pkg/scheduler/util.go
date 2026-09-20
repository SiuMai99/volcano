/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added Unix socket-based HTTP interface for runtime klog level adjustment and debugging
- Improved default scheduler configuration with comprehensive action and plugin setup

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

package scheduler

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"

	utilfeature "k8s.io/apiserver/pkg/util/feature"

	"volcano.sh/volcano/pkg/features"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins"
	xputopologyaware "volcano.sh/volcano/pkg/scheduler/plugins/xpu-topology-aware"
	"volcano.sh/volcano/pkg/scheduler/topology"
	"volcano.sh/volcano/pkg/util"
)

var DefaultSchedulerConf = `
actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: overcommit
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`

// XPUTopologyPluginConfigured reports whether the scheduler configuration
// explicitly opts in to the xPU plugin. It intentionally does not infer
// activation from the feature gate.
func XPUTopologyPluginConfigured(tiers []conf.Tier) bool {
	return len(xputopologyaware.PluginOptions(tiers)) > 0
}

// XPUTopologyActivationConfig builds the static activation state used by the
// process manager. Catalog and Provider runtime readiness remain false until
// their dedicated PRs publish it; this prevents the skeleton from claiming a
// topology capability it does not implement.
func XPUTopologyActivationConfig(tiers []conf.Tier) topology.ActivationConfig {
	options := xputopologyaware.PluginOptions(tiers)
	pluginConfigReady := len(options) > 0
	for _, option := range options {
		if !xputopologyaware.ConfigFromPluginOption(option).Valid() {
			pluginConfigReady = false
			break
		}
	}

	return topology.ActivationConfig{
		GateEnabled:             utilfeature.DefaultFeatureGate.Enabled(features.XPUTopologyAwareScheduling),
		PluginConfigured:        len(options) > 0,
		PluginConfigReady:       pluginConfigReady,
		ConfigurationIdentity:   xputopologyaware.ConfigIdentity(options),
		CatalogReady:            false,
		ProviderReady:           false,
		AssignmentContractReady: false,
	}
}

// XPUTopologyCatalogPath returns the only static catalog source accepted by
// the Alpha plugin configuration. Ambiguous duplicate plugin options are not
// resolved by choosing an arbitrary catalog and therefore remain fail-closed.
func XPUTopologyCatalogPath(tiers []conf.Tier) (string, bool) {
	options := xputopologyaware.PluginOptions(tiers)
	if len(options) != 1 {
		return "", false
	}
	config := xputopologyaware.ConfigFromPluginOption(options[0])
	if !config.Valid() || !config.CatalogPresent {
		return "", false
	}
	return config.CatalogPath, true
}

func UnmarshalSchedulerConf(confStr string) ([]framework.Action, []conf.Tier, []conf.Configuration, map[string]string, error) {
	var actions []framework.Action

	schedulerConf := &conf.SchedulerConfiguration{}

	if err := yaml.Unmarshal([]byte(confStr), schedulerConf); err != nil {
		return nil, nil, nil, nil, err
	}
	// Set default settings for each plugin if not set
	for i, tier := range schedulerConf.Tiers {
		// drf with hierarchy enabled
		hdrf := false
		// proportion enabled
		proportion := false
		for j := range tier.Plugins {
			if tier.Plugins[j].Name == "drf" &&
				tier.Plugins[j].EnabledHierarchy != nil &&
				*tier.Plugins[j].EnabledHierarchy {
				hdrf = true
			}
			if tier.Plugins[j].Name == "proportion" {
				proportion = true
			}
			plugins.ApplyPluginConfDefaults(&schedulerConf.Tiers[i].Plugins[j])
		}
		if hdrf && proportion {
			return nil, nil, nil, nil, fmt.Errorf("proportion and drf with hierarchy enabled conflicts")
		}
	}

	actionNames := strings.Split(schedulerConf.Actions, ",")
	for _, actionName := range actionNames {
		if action, found := framework.GetAction(strings.TrimSpace(actionName)); found {
			actions = append(actions, action)
		} else {
			return nil, nil, nil, nil, fmt.Errorf("failed to find Action %s", actionName)
		}
	}

	return actions, schedulerConf.Tiers, schedulerConf.Configurations, schedulerConf.MetricsConfiguration, nil
}

func runSchedulerSocket() {
	fs := flag.CommandLine
	startKlogLevel := fs.Lookup("v").Value.String()
	socketDir := os.Getenv(util.SocketDirEnvName)
	if socketDir == "" {
		socketDir = util.DefaultSocketDir
	}
	util.ListenAndServeKlogLogLevel("klog", startKlogLevel, socketDir)
}
