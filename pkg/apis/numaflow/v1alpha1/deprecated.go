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

package v1alpha1

import (
	"os"
	"strconv"
)

// TODO: (k8s 1.29) Remove this once we deprecate the support for k8s < 1.29
func isSidecarSupported() bool {
	v := os.Getenv(EnvK8sServerVersion)
	if v == "" {
		return true // default to true if the env var is not found
	}
	// e.g. 1.31
	k8sVersion, _ := strconv.ParseFloat(v, 32)
	return k8sVersion >= 1.29
}
