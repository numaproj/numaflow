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

package generated

import "fmt"

// bindSimpleString copies a simple-style OpenAPI string parameter into dest.
// It replaces github.com/oapi-codegen/runtime binding so the v2 server does
// not pull Echo, Iris, or other unused framework modules into the module graph.
func bindSimpleString(name, value string, dest *string, required bool) error {
	if dest == nil {
		return fmt.Errorf("internal error: destination for parameter %q is nil", name)
	}
	if required && value == "" {
		return fmt.Errorf("query parameter '%s' is required, but was not present", name)
	}
	*dest = value
	return nil
}
