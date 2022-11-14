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
