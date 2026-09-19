#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-volcano-gpu-mvp}"
POD_NAMESPACE="${POD_NAMESPACE:-gpu-mvp}"
POD_NAME="${POD_NAME:-single-h100}"
CONTAINER_NAME="${CONTAINER_NAME:-worker}"
RESOURCE_NAME="${RESOURCE_NAME:-nvidia.com/gpu}"
MANIFEST_PATH="${XPU01_MANIFEST_PATH:-${ROOT_DIR}/evidence/xpu-01/l1-20260917-kind/input-manifest.json}"
OUTPUT_DIR="${XPU01_OUTPUT_DIR:-${ROOT_DIR}/evidence/xpu-01/l1-20260917-kind}"
GOCACHE_DIR="${GOCACHE:-/private/tmp/xpu01-gocache}"
PODRESOURCES_BINARY="${XPU01_PODRESOURCES_BINARY:-}"

die() {
  printf 'xpu-01 PodResources: %s\n' "$*" >&2
  exit 1
}

for required in kubectl jq go kind docker; do
  command -v "${required}" >/dev/null 2>&1 || die "missing required command: ${required}"
done
[[ -f "${MANIFEST_PATH}" ]] || die "L1 manifest not found: ${MANIFEST_PATH}"

k() {
  kubectl --context "${KUBE_CONTEXT}" "$@"
}

pod_json="$(k -n "${POD_NAMESPACE}" get pod "${POD_NAME}" -o json)"
node_name="$(jq -r '.spec.nodeName // empty' <<<"${pod_json}")"
pod_uid="$(jq -r '.metadata.uid // empty' <<<"${pod_json}")"
[[ -n "${node_name}" && -n "${pod_uid}" ]] || die "Pod has no NodeName or UID: ${POD_NAMESPACE}/${POD_NAME}"

node_uid="$(k get node "${node_name}" -o jsonpath='{.metadata.uid}')"
[[ -n "${node_uid}" ]] || die "NodeUID is empty for ${node_name}"
kind_node="$(kind get nodes --name volcano-gpu-mvp | awk -v wanted="${node_name}" '$0 == wanted {print; exit}')"
[[ -n "${kind_node}" ]] || die "kind container not found for node ${node_name}"

binary_path="${PODRESOURCES_BINARY:-/private/tmp/xpu01-podresources-linux-arm64}"
if [[ -z "${PODRESOURCES_BINARY}" ]]; then
  GOCACHE="${GOCACHE_DIR}" GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
    go build -o "${binary_path}" ./tools/xpu-01/podresources
else
  [[ -x "${binary_path}" ]] || die "configured XPU01_PODRESOURCES_BINARY is not executable: ${binary_path}"
fi
container_binary_path="/root/xpu01-podresources"
docker cp "${binary_path}" "${kind_node}:${container_binary_path}"

raw_path="${OUTPUT_DIR}/podresources-${POD_NAME}.json"
pod_snapshot_path="${OUTPUT_DIR}/${POD_NAME}.pod.json"
mkdir -p "${OUTPUT_DIR}"
printf '%s\n' "${pod_json}" >"${pod_snapshot_path}"
docker exec "${kind_node}" "${container_binary_path}" \
  -pod "${POD_NAME}" -namespace "${POD_NAMESPACE}" \
  -container "${CONTAINER_NAME}" -resource "${RESOURCE_NAME}" >"${raw_path}"

expected_device_id="$(jq -r '.selectedDeviceID' "${MANIFEST_PATH}")"
actual_device_count="$(jq '.deviceIDs | length' "${raw_path}")"
actual_device_ids="$(jq -c '.deviceIDs' "${raw_path}")"
inventory_match="$(jq -r --arg node_uid "${node_uid}" --argjson actual "${actual_device_ids}" \
  '[.devices[] | select(.nodeUID == $node_uid) | .deviceID] as $inventory | ($actual | all(. as $id | $inventory | index($id) != null))' \
  "${MANIFEST_PATH}")"
exact_match="$(jq -r --arg expected "${expected_device_id}" --argjson actual "${actual_device_ids}" \
  '($actual | length == 1 and .[0] == $expected)' <<<"{}")"

result_path="${OUTPUT_DIR}/podresources-${POD_NAME}-comparison.json"
jq -n \
  --arg pod_name "${POD_NAME}" \
  --arg namespace "${POD_NAMESPACE}" \
  --arg container_name "${CONTAINER_NAME}" \
  --arg resource_name "${RESOURCE_NAME}" \
  --arg pod_uid "${pod_uid}" \
  --arg node_name "${node_name}" \
  --arg node_uid "${node_uid}" \
  --arg expected_device_id "${expected_device_id}" \
  --argjson actual_device_ids "${actual_device_ids}" \
  --argjson actual_device_count "${actual_device_count}" \
  --argjson inventory_match "${inventory_match}" \
  --argjson exact_match "${exact_match}" \
  '{
    podName: $pod_name,
    namespace: $namespace,
    containerName: $container_name,
    resourceName: $resource_name,
    podUID: $pod_uid,
    nodeName: $node_name,
    nodeUID: $node_uid,
    expectedSelectedDeviceID: $expected_device_id,
    actualDeviceIDs: $actual_device_ids,
    actualDeviceCount: $actual_device_count,
    actualIDsBelongToNodeInventory: $inventory_match,
    actualIDsEqualExpectedSelectedID: $exact_match
  }' >"${result_path}"

if [[ "${actual_device_count}" -eq 0 || "${inventory_match}" != "true" ]]; then
  die "PodResources returned no device or a device outside the node inventory; inspect ${result_path}"
fi

printf 'PodResources observed %s device(s) from node %s.\n' "${actual_device_count}" "${node_name}"
if [[ "${exact_match}" == "true" ]]; then
  printf 'Exact selected UUID matched: %s\n' "${expected_device_id}"
else
  printf 'Exact selected UUID did not match; stock Device Plugin path has no selected-ID enforcement.\n'
fi
printf 'Raw: %s\nCompare: %s\n' "${raw_path}" "${result_path}"
