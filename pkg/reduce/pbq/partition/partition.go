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

// Package partition is a tuple containing (start, end) time and an optional slot.
// Window contains a partition because Window contains the keys too.
// A partition is used to map a message to a pbq instance.
package partition

import (
	"fmt"
	"time"
)

// ID uniquely identifies a partition.
type ID struct {
	Start time.Time
	End   time.Time
	// Slot is a hash-range for keys (multiple keys can go to the same slot)
	Slot string
}

func (p ID) String() string {
	return fmt.Sprintf("%v-%v-%s", p.Start.UnixMilli(), p.End.UnixMilli(), p.Slot)
}
