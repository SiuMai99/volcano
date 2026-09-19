# XPU-01 L0 Provider identity probe

This directory contains a small, standard-library-only contract harness for
XPU-01. It is deliberately a test harness, not a production scheduler plugin
or NVIDIA Device Plugin implementation.

It verifies the fixed XPU-00 D4 identity contract:

```text
Vendor=NVIDIA
ProviderID=nvidia-nvml-v1
ResourceName=nvidia.com/gpu
DiscoveryAPI=NVML
```

The harness selects a non-default mock `DeviceID`, binds it to the requested
NodeUID, emits the minimum `version/resourceName/provider/deviceKeys`
assignment, parses it strictly, and checks that it round-trips canonically.
It fails closed when the provider cannot confirm the selected ID, the ID is
not on the requested NodeUID, or the device is unhealthy.

## Run the L0 probe

From the Volcano repository root:

```bash
go test ./tools/xpu-01
go run ./tools/xpu-01 -input ./tools/xpu-01/testdata/mock-selected-uuid.json
```

The command prints a structured JSON result. A non-`Pass` result exits with a
non-zero status. The `L0` result proves only the contract harness behavior; it
does not prove that the NVIDIA Device Plugin or kubelet consumed the selected
UUID.

## Relationship to the kind MVP

The `volcano-device-plugin-mvp` project is the planned L1 input source:

```text
nvml-mock profile -> NVIDIA Device Plugin ListAndWatch/Allocate
                  -> Node nvidia.com/gpu capacity and mock inventory
```

Its existing `make inventory` output can populate a future L1 manifest, but
the current MVP does not provide a scheduler-selected UUID input channel or
`volcano.sh/xpu-assignment` producer. Therefore an L1 run must report the
Device Plugin compatibility result separately. If selected UUID confirmation
is unavailable, the correct XPU-01 conclusion is
`XPUAssignmentNotEnforceable`.

## Run the minimal Provider-Adapter bridge

The `bridge` mode is a positive contract-feasibility harness over the same
manifest inventory:

```bash
go run ./tools/xpu-01 \
  -mode bridge \
  -input ./evidence/xpu-01/l1-20260917-kind/input-manifest.json
```

It passes scheduler-owned `NodeUID/DeviceKey` values unchanged to a mock
NVIDIA Provider, confirms exact inventory membership and health, and emits the
minimum `volcano.sh/xpu-assignment` JSON value. It does not mutate a Pod, call
kubelet, or prove that the stock Device Plugin will enforce the selected UUID.
The same manifest should still return
`XPUAssignmentNotEnforceable` in the default `stock-probe` mode.

Real NVIDIA runtime verification is a separate L2 gate and cannot be replaced
by this harness or by `nvml-mock` output.

## Collect L1 evidence from the kind MVP

After the external MVP cluster is available, run from the Volcano repository
root:

```bash
./tools/xpu-01/collect-kind-l1.sh
```

The collector invokes the MVP's existing `verify.sh` and `inventory.sh`, reads
the three mock pods' UUIDs and NodeUIDs, and writes an immutable input manifest
under `evidence/xpu-01/`. It intentionally records the stock Device Plugin
path as unable to confirm a scheduler-selected UUID, so the expected result is
`XPUAssignmentNotEnforceable`. This is useful L1 compatibility evidence; it is
not a successful exact-ID result. Set `MVP_ROOT` or `KUBE_CONTEXT` only when
using a deliberately different environment.

## Read actual kubelet DeviceIDs

With a GPU Pod running in the kind MVP, read the kubelet PodResources gRPC
socket from the worker container:

```bash
GOCACHE=/private/tmp/xpu01-gocache ./tools/xpu-01/probe-podresources-kind.sh
```

This produces a raw PodResources observation and a comparison under the L1
evidence directory. It proves which mock `DeviceIDs` kubelet assigned to the
Pod and whether those IDs belong to the node inventory. It does not prove
Volcano selected them: the comparison must separately equal the expected
scheduler-selected ID, and a mismatch is the expected stock Device Plugin
compatibility gap.

If the host cannot resolve the repository's full Go module graph, build a
Linux/arm64 client in a suitable Go environment and pass it explicitly with
`XPU01_PODRESOURCES_BINARY=/path/to/client`.
