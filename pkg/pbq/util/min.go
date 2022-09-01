package util

// Min returns the min value
func Min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
