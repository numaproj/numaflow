/*
Copyright 2026 The Numaproj Authors.

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

// Package capabilities advertises API v2 operations and server-enforced limits.
// It does not define user interface preferences or feature-rollout policy.
package capabilities

// Limits defines server-enforced bounds that API v2 clients must observe.
type Limits struct {
	DefaultPageSize     int
	MaximumPageSize     int
	MaximumLogLines     int
	MaximumMetricPoints int
}

// Capabilities describes the API v2 operations and limits available from a server.
type Capabilities struct {
	APIVersion string
	Operations []string
	Limits     Limits
}
