/*
Copyright 2022 The Numaproj Authors.

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

package serverinfo

type Language string

const (
	Go     Language = "go"
	Python Language = "python"
	Java   Language = "java"
)

type sdkConstraints map[Language]string

var minimumSupportedSDKVersions = sdkConstraints{
	Go:     "0.8.0",
	Python: "0.8.0",
	Java:   "0.8.0",
}

type Protocol string

const (
	UDS Protocol = "uds"
	TCP Protocol = "tcp"
)

type MapMode string

const (
	UnaryMap  MapMode = "unary-map"
	StreamMap MapMode = "stream-map"
	BatchMap  MapMode = "batch-map"
)

// MapModeKey is the key used in the server info metadata map to indicate which map mode is enabled.
const MapModeKey = "MAP_MODE"

// MinimumNumaflowVersion specifies the minimum Numaflow version required by the current SDK version
const MinimumNumaflowVersion = "1.3.0-rc1"

// ServerInfo is the information about the server
type ServerInfo struct {
	Protocol               Protocol          `json:"protocol"`
	Language               Language          `json:"language"`
	MinimumNumaflowVersion string            `json:"minimum_numaflow_version"`
	Version                string            `json:"version"`
	Metadata               map[string]string `json:"metadata"`
}
