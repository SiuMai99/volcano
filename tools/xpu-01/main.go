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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	inputPath := flag.String("input", "", "path to an XPU-01 JSON input manifest")
	outputPath := flag.String("output", "", "optional path for the structured JSON result")
	mode := flag.String("mode", "stock-probe", "probe mode: stock-probe or bridge")
	flag.Parse()
	if *inputPath == "" {
		fail("-input is required")
	}

	inputData, err := os.ReadFile(*inputPath)
	if err != nil {
		fail("read input: %v", err)
	}
	var input ProbeInput
	decoder := json.NewDecoder(bytes.NewReader(inputData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		fail("decode input: %v", err)
	}
	if err := ensureEOF(decoder); err != nil {
		fail("decode trailing input: %v", err)
	}

	var result interface{}
	var status string
	switch *mode {
	case "stock-probe":
		probeResult := RunProbe(input)
		result = probeResult
		status = probeResult.Status
	case "bridge":
		bridgeResult := RunBridge(input)
		result = bridgeResult
		status = bridgeResult.Status
	default:
		fail("unsupported -mode %q; want stock-probe or bridge", *mode)
	}

	resultData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fail("encode result: %v", err)
	}
	resultData = append(resultData, '\n')
	if *outputPath != "" {
		if err := os.WriteFile(*outputPath, resultData, 0o644); err != nil {
			fail("write result: %v", err)
		}
	}
	if _, err := os.Stdout.Write(resultData); err != nil {
		fail("write stdout: %v", err)
	}
	if status != StatusPass {
		os.Exit(1)
	}
}

func ensureEOF(decoder *json.Decoder) error {
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("input contains more than one JSON value")
		}
		return err
	}
	return nil
}

func fail(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, "xpu-01: "+format+"\n", args...)
	os.Exit(2)
}
