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
	rwLock             *sync.RWMutex
}

// IsShuttingDown returns whether we can stop processing.
func (df *DataForward) IsShuttingDown() (bool, error) {
	df.Shutdown.rwLock.RLock()
	defer df.Shutdown.rwLock.RUnlock()

	if df.Shutdown.forceShutdown || df.Shutdown.startShutdown {
		return true, nil
	}

	return false, nil
}

func (s *Shutdown) String() string {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return fmt.Sprintf("startShutdown:%t forceShutdown:%t shutdownRequestCtr:%d initiateTime:%s",
		s.startShutdown, s.forceShutdown, s.shutdownRequestCtr, s.initiateTime)
}

// Stop stops the processing.
func (df *DataForward) Stop() {
	df.Shutdown.rwLock.Lock()
	defer df.Shutdown.rwLock.Unlock()
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
	df.Shutdown.rwLock.Lock()
	defer df.Shutdown.rwLock.Unlock()
	df.Shutdown.forceShutdown = true
}
