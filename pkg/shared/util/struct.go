package util

import "reflect"

// IsZeroStruct checks if a struct is zero value.
func IsZeroStruct(v interface{}) bool {
	val := reflect.ValueOf(v)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if !field.IsZero() {
			return false
		}
	}
	return true
}
