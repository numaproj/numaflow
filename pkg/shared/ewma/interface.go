package ewma

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
