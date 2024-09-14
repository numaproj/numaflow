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
	Rust   Language = "rust"
)

type sdkConstraints map[Language]string

var minimumSupportedSDKVersions = sdkConstraints{
	Go:     "0.8.0",
	Python: "0.8.0",
	Java:   "0.8.0",
	Rust:   "0.1.0",
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

// Metadata keys used in the server info file
const (
	// MultiProcKey is the field used to indicate that MultiProc map mode is enabled
	// The value contains the number of servers spawned.
	MultiProcKey = "MULTIPROC"
	// MapModeKey field is used to indicate which map mode is enabled
	// If none is set, we consider the unary map as default
	MapModeKey = "MAP_MODE"
)

// ServerInfo is the information about the server
type ServerInfo struct {
	Protocol               Protocol          `json:"protocol"`
	Language               Language          `json:"language"`
	MinimumNumaflowVersion string            `json:"minimum_numaflow_version"`
	Version                string            `json:"version"`
	Metadata               map[string]string `json:"metadata"`
}
