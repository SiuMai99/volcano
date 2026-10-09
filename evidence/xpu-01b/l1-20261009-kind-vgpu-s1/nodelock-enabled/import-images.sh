#!/usr/bin/env bash
set -Eeuo pipefail

archive=/private/tmp/xpu01b-lock-1009/images.tar
docker save --output "$archive" \
  ghcr.io/nvidia/nvml-mock:latest \
  volcanosh/vc-controller-manager:v1.15.2 \
  volcanosh/vc-webhook-manager:v1.15.2 \
  volcano-vgpu-uuid/vc-scheduler:dev \
  projecthami/volcano-vgpu-device-plugin:v1.12.0 \
  busybox:1.36.1
for node in xpu01b-lock-1009-control-plane xpu01b-lock-1009-worker; do
  docker exec --privileged -i "$node" \
    ctr --namespace=k8s.io images import \
    --platform linux/arm64 \
    --snapshotter=overlayfs - < "$archive"
done
rm -- "$archive"
