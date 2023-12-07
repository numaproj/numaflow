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

package forward

import (
	"fmt"
	"sync"
	"time"
)

// Shutdown tracks and enforces the shutdown activity.
type Shutdown struct {
	startShutdown      bool
	forceShutdown      bool
	initiateTime       time.Time
	shutdownRequestCtr int
	rwlock             *sync.RWMutex
}

// IsShuttingDown returns whether we can stop processing.
func (df *DataForward) IsShuttingDown() (bool, error) {
	df.Shutdown.rwlock.RLock()
	defer df.Shutdown.rwlock.RUnlock()

	if df.Shutdown.forceShutdown || df.Shutdown.startShutdown {
		return true, nil
	}

	return false, nil
}

func (s *Shutdown) String() string {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return fmt.Sprintf("startShutdown:%t forceShutdown:%t shutdownRequestCtr:%d initiateTime:%s",
		s.startShutdown, s.forceShutdown, s.shutdownRequestCtr, s.initiateTime)
}

// Stop stops the processing.
func (df *DataForward) Stop() {
	df.Shutdown.rwlock.Lock()
	defer df.Shutdown.rwlock.Unlock()
	if df.Shutdown.initiateTime.IsZero() {
		df.Shutdown.initiateTime = time.Now()
	}
	df.Shutdown.startShutdown = true
	df.Shutdown.shutdownRequestCtr++
	// call cancel
	df.cancelFn()
}

// ForceStop sets up the force shutdown flag.
func (df *DataForward) ForceStop() {
	// call stop (what if we have an enthusiastic shutdown that forces first)
	// e.g., I know I have written a wrong source transformer, so shutdown ASAP
	df.Stop()
	df.Shutdown.rwlock.Lock()
	defer df.Shutdown.rwlock.Unlock()
	df.Shutdown.forceShutdown = true
}
