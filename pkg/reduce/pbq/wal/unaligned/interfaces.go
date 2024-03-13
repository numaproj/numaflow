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

package unaligned

import (
	"context"

	"github.com/numaproj/numaflow/pkg/window"
)

// Compactor compacts the unalignedWAL by deleting the persisted messages
// which belongs to the materialized window.
type Compactor interface {
	// Start starts the compactor
	Start(ctx context.Context) error
	// Stop stops the compactor
	Stop() error
}

// GCEventsWAL persists the GC events from PnF of unaligned windows.
type GCEventsWAL interface {
	// PersistGCEvent persists the GC event of the window
	PersistGCEvent(window window.TimedWindow) error
	// Close closes the GCEventsWAL
	Close() error
}
