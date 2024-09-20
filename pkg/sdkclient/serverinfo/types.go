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

import "strings"

type Language string

const (
	Go     Language = "go"
	Python Language = "python"
	Java   Language = "java"
	Rust   Language = "rust"
)

type sdkConstraints map[Language]string

/*
minimumSupportedSDKVersions is the minimum supported version of each SDK for the current numaflow version.
It is used to check if the SDK is compatible with the current numaflow version.

NOTE: when updating it, please also update MINIMUM_SUPPORTED_SDK_VERSIONS for mono vertex at rust/monovertex/server_info.rs

Python SDK versioning follows PEP 440 (https://www.python.org/dev/peps/pep-0440/).
The other SDKs follow the semver versioning scheme (https://semver.org/).

How to update this map:

There are two types of releases, one is the stable release and the other is the pre-release.
Below are the typical formats of the versioning scheme:

	+------------------+-------------------------+-----------------------------+
	|                  |          PEP 440        |            semver           |
	+------------------+-------------------------+-----------------------------+
	|      stable      |          0.8.0          |           0.8.0             |
	+------------------+-------------------------+-----------------------------+
	|   pre-release    |    0.8.0a1,             |       0.8.0-rc1,            |
	|                  |    0.8.0b3,             |  0.8.0-0.20240913163521,    |
	|                  |    or 0.8.0rc1          |            etc.             |
	+------------------+-------------------------+-----------------------------+

There are two cases to consider when updating the map:

1. The minimum supported version is a pre-release version.
In this case, directly put the exact pre-release version in the map.
E.g., if the minimum supported version is "0.8.0-rc1", then put "0.8.0-rc1" for java, go, rust.
"0.8.0b1", "0.8.0b1" for python.
2. The minimum supported version is a stable version.
In this case, put (almost) the largest available pre-release version of the stable version in the map.
This is because the go semver library considers pre-releases to be invalid if the constraint range does not include pre-releases.
Therefore, we have to put a pre-release version of the stable version in the map and choose the largest one.
For python, we use "rc100" as the largest pre-release version. For go, rust, we use "-z" as the largest pre-release version.
E.g., if the minimum supported version is "0.8.0", then put "0.8.0-z" for java, go, rust, "0.8.0rc100" for python.
A constraint ">=0.8.0-z" will match any pre-release version of 0.8.0, including "0.8.0-rc1", "0.8.0-rc2", etc.

More details about version comparison can be found in the PEP 440 and semver documentation.
*/
var minimumSupportedSDKVersions = sdkConstraints{
	// meaning the minimum supported python SDK version is 0.8.0
	Python: "0.8.0rc100",
	// meaning the minimum supported go SDK version is 0.8.0
	Go: "0.8.0-z",
	// meaning the minimum supported java SDK version is 0.8.0
	Java: "0.8.0-z",
	// meaning the minimum supported rust SDK version is 0.1.0
	Rust: "0.1.0-z",
}

// humanReadable returns the human-readable minimum supported version.
// it's used for logging purposes.
// it translates the version we used in the constraints to the real minimum supported version.
// e.g., if the given version is "0.8.0rc100", human-readable version is "0.8.0".
// if the given version is "0.8.0-z", "0.8.0".
// if "0.8.0-rc1", "0.8.0-rc1".
func humanReadable(ver string) string {
	if ver == "" {
		return ""
	}
	// semver
	if strings.HasSuffix(ver, "-z") {
		return ver[:len(ver)-2]
	}
	// PEP 440
	if strings.HasSuffix(ver, "rc100") {
		return ver[:len(ver)-5]
	}
	return ver
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
