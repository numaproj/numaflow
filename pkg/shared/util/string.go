package util

import (
	"crypto/rand"
	"math/big"
	"strings"
)

// generate a random string with given length
func RandomString(length int) string {
	seeds := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(seeds))))
		result[i] = seeds[num.Int64()]
	}
	return string(result)
}

func RandomLowerCaseString(length int) string {
	return strings.ToLower(RandomString(length))
}

func StringSliceContains(list []string, str string) bool {
	if len(list) == 0 {
		return false
	}
	for _, s := range list {
		if s == str {
			return true
		}
	}
	return false
}
