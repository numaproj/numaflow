package ewma

// EWMA is the interface for Exponentially Weighted Moving Average
// It is used to calculate the moving average with decay of a series of numbers
type EWMA interface {
	// Add adds a new value to the EWMA
	Add(float64)
	// Get returns the current value of the EWMA
	Get() float64
	// Reset resets the EWMA to the initial value
	Reset()
	// Set sets the EWMA to the given value
	Set(float64)
}
