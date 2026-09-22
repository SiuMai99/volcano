#!/usr/bin/env bash

# Verify that the PR8 Helm example is an explicit opt-in and renders the
# install-time contract needed by the M2 xPU topology Advisory MVP.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart_path="${repo_root}/installer/helm/chart/volcano"
example_values="${chart_path}/examples/xpu-topology-aware-values.yaml"
opt_in_render=$(mktemp "${TMPDIR:-/tmp}/volcano-xpu-opt-in.XXXXXX.yaml")
default_render=$(mktemp "${TMPDIR:-/tmp}/volcano-xpu-default.XXXXXX.yaml")
trap 'rm -f "${opt_in_render}" "${default_render}"' EXIT

helm lint "${chart_path}" -f "${example_values}"
helm template volcano "${chart_path}" --namespace volcano-system -f "${example_values}" >"${opt_in_render}"
helm template volcano "${chart_path}" --namespace volcano-system >"${default_render}"

for required in \
  'name: volcano-xpu-topology-catalog' \
  'XPUTopologyAwareScheduling=true' \
  'name: xpu-topology-aware' \
  'xpu-topology.catalog: /volcano.xpu/catalog.json' \
  'resources: ["podgroups"]' \
  'verbs: ["get", "list", "watch", "update"]' \
  'resources: ["podgroups/status"]' \
  'verbs: ["update", "patch"]' \
  'mountPath: /volcano.xpu'; do
  if ! grep -Fq "${required}" "${opt_in_render}"; then
    echo "missing required xPU Helm render fragment: ${required}" >&2
    exit 1
  fi
done

if grep -Eq 'xpu-topology-aware|xpu-topology-catalog|XPUTopologyAwareScheduling' "${default_render}"; then
  echo "default Helm render unexpectedly enables xPU topology" >&2
  exit 1
fi

echo "xPU topology Helm opt-in render verified"
