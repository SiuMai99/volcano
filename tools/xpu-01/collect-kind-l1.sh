#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MVP_ROOT="${MVP_ROOT:-/Users/oujunwei/Documents/hami/volcano-device-plugin-mvp}"
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-volcano-gpu-mvp}"
OUTPUT_DIR="${XPU01_OUTPUT_DIR:-${ROOT_DIR}/evidence/xpu-01/l1-$(date -u +%Y%m%dT%H%M%SZ)}"

die() {
  printf 'xpu-01 L1: %s\n' "$*" >&2
  exit 1
}

for required in kubectl jq go kind; do
  command -v "${required}" >/dev/null 2>&1 || die "missing required command: ${required}"
done
[[ -x "${MVP_ROOT}/scripts/verify.sh" ]] || die "MVP_ROOT does not contain scripts/verify.sh: ${MVP_ROOT}"
[[ -x "${MVP_ROOT}/scripts/inventory.sh" ]] || die "MVP_ROOT does not contain scripts/inventory.sh: ${MVP_ROOT}"

k() {
  kubectl --context "${KUBE_CONTEXT}" "$@"
}

mkdir -p "${OUTPUT_DIR}"

printf 'Collecting kind L1 evidence in %s\n' "${OUTPUT_DIR}"
k get nodes >/dev/null || die "Kubernetes context is not reachable: ${KUBE_CONTEXT}"

environment_path="${OUTPUT_DIR}/environment.txt"
{
  printf 'collectedAtUTC='; date -u +%Y-%m-%dT%H:%M:%SZ
  printf 'repositoryCommit='; git -C "${ROOT_DIR}" rev-parse HEAD
  printf 'repositoryBranch='; git -C "${ROOT_DIR}" branch --show-current
  printf 'goVersion='; go version
  printf 'kindVersion='; kind version
  printf '%s\n' 'kubectlClientVersion:'
  kubectl version --client
  printf '%s\n' 'clusterVersion:'
  k version
  printf '%s\n' 'nodes:'
  k get nodes -o wide
  printf '%s\n' 'nvmlMockPods:'
  k -n nvml-mock-system get pods -o wide
  printf '%s\n' 'devicePluginPods:'
  k get pods -A -l app.kubernetes.io/name=nvidia-device-plugin-mock -o wide
  printf '%s\n' 'volcanoPods:'
  k -n volcano-system get pods -o wide
} >"${environment_path}" 2>&1

verify_status=0
"${MVP_ROOT}/scripts/verify.sh" >"${OUTPUT_DIR}/mvp-verify.log" 2>&1 || verify_status=$?
if [[ "${verify_status}" -ne 0 ]]; then
  last_verify_line="$(tail -n 1 "${OUTPUT_DIR}/mvp-verify.log")"
  if [[ "${last_verify_line}" != "ERROR: PodGroup CRD is unavailable" ]] || \
    ! k api-resources --api-group=scheduling.volcano.sh | awk '$1 == "podgroups" {found=1} END {exit found ? 0 : 1}'; then
    die "MVP verify failed; inspect ${OUTPUT_DIR}/mvp-verify.log"
  fi
  printf '%s\n' \
    'collector note: ignored the MVP verify.sh PodGroup false negative; direct api-resources check confirmed podgroups.' \
    >>"${OUTPUT_DIR}/mvp-verify.log"
fi
if ! "${MVP_ROOT}/scripts/inventory.sh" >"${OUTPUT_DIR}/mvp-inventory.log" 2>&1; then
  die "MVP inventory failed; inspect ${OUTPUT_DIR}/mvp-inventory.log"
fi

node_names=(
  "volcano-gpu-mvp-worker"
  "volcano-gpu-mvp-worker2"
  "volcano-gpu-mvp-worker3"
)
release_names=(nvml-h100 nvml-l40s nvml-t4)
devices_json='[]'
selected_node="${node_names[0]}"
selected_node_uid="$(k get node "${selected_node}" -o jsonpath='{.metadata.uid}')"
[[ -n "${selected_node_uid}" ]] || die "NodeUID is empty for ${selected_node}"

for index in "${!node_names[@]}"; do
  node_name="${node_names[${index}]}"
  release_name="${release_names[${index}]}"
  node_uid="$(k get node "${node_name}" -o jsonpath='{.metadata.uid}')"
  [[ -n "${node_uid}" ]] || die "NodeUID is empty for ${node_name}"
  mock_pod="$(k -n nvml-mock-system get pods \
    -l "app.kubernetes.io/instance=${release_name},app.kubernetes.io/component=daemon" \
    --field-selector "spec.nodeName=${node_name}" \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "${mock_pod}" ]] || die "nvml-mock pod not found for ${node_name}"

  while IFS= read -r device_id; do
    device_id="$(printf '%s' "${device_id}" | tr -d '[:space:]')"
    [[ -n "${device_id}" ]] || continue
    devices_json="$(jq -c \
      --arg node_name "${node_name}" \
      --arg node_uid "${node_uid}" \
      --arg device_id "${device_id}" \
      '. + [{nodeName: $node_name, nodeUID: $node_uid, deviceID: $device_id, healthy: true}]' \
      <<<"${devices_json}")"
  done < <(k -n nvml-mock-system exec "${mock_pod}" -- \
    nvidia-smi --query-gpu=uuid --format=csv,noheader)
done

selected_device_id="$(jq -r --arg node_name "${selected_node}" \
  '[.[] | select(.nodeName == $node_name)][1].deviceID // empty' <<<"${devices_json}")"
[[ -n "${selected_device_id}" ]] || die "selected non-default device was not found on ${selected_node}"

manifest_path="${OUTPUT_DIR}/input-manifest.json"
jq -n \
  --arg case_id "nvidia-nvml-mock-selected-uuid-idempotent" \
  --arg evidence_level "L1" \
  --arg node_name "${selected_node}" \
  --arg node_uid "${selected_node_uid}" \
  --arg selected_device_id "${selected_device_id}" \
  --argjson devices "${devices_json}" \
  '{
    caseID: $case_id,
    evidenceLevel: $evidence_level,
    contract: {
      vendor: "NVIDIA",
      providerID: "nvidia-nvml-v1",
      namespace: "nvidia.com",
      resourceName: "nvidia.com/gpu",
      discoveryAPI: "NVML"
    },
    nodeName: $node_name,
    nodeUID: $node_uid,
    devices: $devices,
    selectedDeviceID: $selected_device_id,
    providerCanConfirmSelected: false
  }' >"${manifest_path}"

printf '%s\n' \
  'providerCanConfirmSelected=false is intentional: the stock NVIDIA Device Plugin path exposes capacity and Allocate, but has no scheduler-selected UUID confirmation channel.' \
  'The resulting XPUAssignmentNotEnforceable is a compatibility-gap observation, not a mock inventory failure.' \
  >"${OUTPUT_DIR}/capability-note.txt"

result_path="${OUTPUT_DIR}/probe-result.json"
set +e
go run ./tools/xpu-01 -input "${manifest_path}" -output "${result_path}" \
  >"${OUTPUT_DIR}/probe.stdout" 2>"${OUTPUT_DIR}/probe.stderr"
probe_exit=$?
set -e

[[ "${probe_exit}" -eq 1 ]] || die "expected stock Device Plugin compatibility probe to fail closed, exit=${probe_exit}; inspect ${OUTPUT_DIR}"
[[ "$(jq -r '.status' "${result_path}")" == "Fail" ]] || die "probe result is not Fail"
[[ "$(jq -r '.reason' "${result_path}")" == "XPUAssignmentNotEnforceable" ]] || die "unexpected probe reason in ${result_path}"

bridge_result_path="${OUTPUT_DIR}/bridge-result.json"
set +e
go run ./tools/xpu-01 -mode bridge -input "${manifest_path}" -output "${bridge_result_path}" \
  >"${OUTPUT_DIR}/bridge.stdout" 2>"${OUTPUT_DIR}/bridge.stderr"
bridge_exit=$?
set -e

[[ "${bridge_exit}" -eq 0 ]] || die "mock Provider-Adapter bridge failed, exit=${bridge_exit}; inspect ${OUTPUT_DIR}"
[[ "$(jq -r '.status' "${bridge_result_path}")" == "Pass" ]] || die "bridge result is not Pass"
[[ "$(jq -r '.assignmentAnnotationKey' "${bridge_result_path}")" == "volcano.sh/xpu-assignment" ]] || \
  die "bridge did not emit the expected assignment annotation key"

printf 'L1 inventory collected; stock Device Plugin gap recorded as XPUAssignmentNotEnforceable.\n'
printf 'Manifest: %s\nStock result: %s\nBridge result: %s\n' \
  "${manifest_path}" "${result_path}" "${bridge_result_path}"
