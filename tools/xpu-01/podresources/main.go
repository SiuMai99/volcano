// Copyright 2026 The Volcano Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	podresources "k8s.io/kubelet/pkg/apis/podresources/v1"
)

type observation struct {
	PodName       string   `json:"podName"`
	Namespace     string   `json:"namespace"`
	ContainerName string   `json:"containerName"`
	ResourceName  string   `json:"resourceName"`
	DeviceIDs     []string `json:"deviceIDs"`
}

func main() {
	socketPath := flag.String("socket", "/var/lib/kubelet/pod-resources/kubelet.sock", "kubelet PodResources gRPC socket")
	podName := flag.String("pod", "", "Pod name")
	namespace := flag.String("namespace", "", "Pod namespace")
	containerName := flag.String("container", "", "container name")
	resourceName := flag.String("resource", "nvidia.com/gpu", "resource name")
	flag.Parse()
	if *podName == "" || *namespace == "" || *containerName == "" {
		fail("-pod, -namespace and -container are required")
	}

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", *socketPath)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "unix://"+*socketPath,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fail("dial PodResources socket: %v", err)
	}
	defer conn.Close()

	response, err := podresources.NewPodResourcesListerClient(conn).List(ctx, &podresources.ListPodResourcesRequest{})
	if err != nil {
		fail("list PodResources: %v", err)
	}
	for _, pod := range response.GetPodResources() {
		if pod.GetName() != *podName || pod.GetNamespace() != *namespace {
			continue
		}
		for _, container := range pod.GetContainers() {
			if container.GetName() != *containerName {
				continue
			}
			for _, device := range container.GetDevices() {
				if device.GetResourceName() != *resourceName {
					continue
				}
				writeObservation(observation{
					PodName:       *podName,
					Namespace:     *namespace,
					ContainerName: *containerName,
					ResourceName:  *resourceName,
					DeviceIDs:     device.GetDeviceIds(),
				})
				return
			}
		}
	}

	fail("PodResources entry not found for %s/%s container=%s resource=%s", *namespace, *podName, *containerName, *resourceName)
}

func writeObservation(value observation) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fail("encode observation: %v", err)
	}
	_, _ = os.Stdout.Write(append(data, '\n'))
}

func fail(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, "xpu-01 podresources: "+format+"\n", args...)
	os.Exit(2)
}
