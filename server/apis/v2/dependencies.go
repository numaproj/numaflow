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

package v2

import "github.com/numaproj/numaflow/server/application/podview"

// NewClusterHandler assembles the API v2 handler from server configuration.
// PR1 does not require a Kubernetes client because capabilities are static.
func NewClusterHandler(mode podview.Mode) (*Handler, error) {
	service, err := podview.NewService(mode)
	if err != nil {
		return nil, err
	}
	return NewHandler(service)
}
