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

package v1alpha1

import "testing"

func TestSqsSourceDeepCopyCopiesMaxReceiveCount(t *testing.T) {
	maxReceiveCount := int32(5)
	source := &SqsSource{MaxReceiveCount: &maxReceiveCount}

	copy := source.DeepCopy()
	if copy.MaxReceiveCount == source.MaxReceiveCount {
		t.Fatal("DeepCopy() reused the MaxReceiveCount pointer")
	}

	*copy.MaxReceiveCount = 10
	if got := *source.MaxReceiveCount; got != 5 {
		t.Fatalf("mutating the copy changed the original MaxReceiveCount to %d", got)
	}
}
