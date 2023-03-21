package wmb

type WMBChecker struct {
	counter int
	max     int
	w       WMB
}

// NewWMBChecker returns a WMBChecker to check if the wmb is idle.
// If all the iterations get the same wmb, the wmb is considered as valid
// and will be used to publish a wmb to pods of the next vertex.
func NewWMBChecker(numOfIteration int) WMBChecker {
	return WMBChecker{
		counter: 0,
		max:     numOfIteration,
		w:       WMB{},
	}
}

// ValidateHeadWMB checks if the head wmb is idle and is the same as the wmb from the previous iteration.
// If all the iterations get the same wmb, returns true.
func (c *WMBChecker) ValidateHeadWMB(w WMB) bool {
	if !w.Idle {
		// if wmb is not idle, skip and reset the counter
		c.counter = 0
		return false
	}
	// check the counter value
	if c.counter == 0 {
		c.counter++
		// the wmb only writes once when counter is zero
		c.w = w
	} else if c.counter < c.max-1 {
		c.counter++
		if c.w == w {
			// we get the same wmb, meaning the wmb is valid, continue
		} else {
			// else, start over
			c.counter = 0
		}
	} else if c.counter >= c.max-1 {
		c.counter = 0
		if c.w == w {
			// reach max iteration, if still get the same wmb,
			// then the wmb is considered as valid, return ture
			return true
		}
	}
	return false
}

// GetCounter gets the current counter value for the WMBChecker, it's used in log and tests
func (c *WMBChecker) GetCounter() int {
	return c.counter
}
