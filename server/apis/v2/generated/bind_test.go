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

import "testing"

func TestBindSimpleString(t *testing.T) {
	t.Run("required empty", func(t *testing.T) {
		var dest string
		if err := bindSimpleString("namespace", "", &dest, true); err == nil {
			t.Fatal("expected error for empty required parameter")
		}
	})
	t.Run("required value", func(t *testing.T) {
		var dest string
		if err := bindSimpleString("namespace", "default", &dest, true); err != nil {
			t.Fatal(err)
		}
		if dest != "default" {
			t.Fatalf("got %q", dest)
		}
	})
	t.Run("optional empty", func(t *testing.T) {
		var dest string
		if err := bindSimpleString("If-None-Match", "", &dest, false); err != nil {
			t.Fatal(err)
		}
		if dest != "" {
			t.Fatalf("got %q", dest)
		}
	})
}
