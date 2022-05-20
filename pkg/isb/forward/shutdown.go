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
func (isdf *InterStepDataForward) IsShuttingDown() (bool, error) {
	isdf.Shutdown.rwlock.RLock()
	defer isdf.Shutdown.rwlock.RUnlock()

	if isdf.Shutdown.forceShutdown || isdf.Shutdown.startShutdown {
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
func (isdf *InterStepDataForward) Stop() {
	isdf.Shutdown.rwlock.Lock()
	defer isdf.Shutdown.rwlock.Unlock()
	if isdf.Shutdown.initiateTime.IsZero() {
		isdf.Shutdown.initiateTime = time.Now()
	}
	isdf.Shutdown.startShutdown = true
	isdf.Shutdown.shutdownRequestCtr++
	// call cancel
	isdf.cancelFn()
}

// ForceStop sets up the force shutdown flag.
func (isdf *InterStepDataForward) ForceStop() {
	// call stop (what if we have an enthusiastic shutdown that forces first)
	// eg. I know I have written a wrong UDF shutdown ASAP
	isdf.Stop()
	isdf.Shutdown.rwlock.Lock()
	defer isdf.Shutdown.rwlock.Unlock()
	isdf.Shutdown.forceShutdown = true
}
