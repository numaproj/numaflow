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
	"hash/crc32"
	"math/big"
	"regexp"
	"slices"
	"strings"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// RandomString generate a random string with given length
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

// CompareSlice compares two slices based on operator.
// OP == AND returns true if all the elements of slice a are in slice b
// OP = OR returns true if any of the elements of slice a are in slice b
// OP = NOT returns false if any of the elements of slice a are in slice b
func CompareSlice(operator v1alpha1.LogicOperator, a []string, b []string) bool {
	switch operator {
	case v1alpha1.LogicOperatorAnd:
		// returns true if all the elements of slice a are in slice b
		for _, val := range a {
			if !slices.Contains(b, val) {
				return false
			}
		}
		return true

	case v1alpha1.LogicOperatorOr:
		// returns true if any of the elements of slice a are in slice b
		for _, val := range a {
			if slices.Contains(b, val) {
				return true
			}
		}
		return false

	case v1alpha1.LogicOperatorNot:
		// returns false if any of the elements of slice a are in slice b
		for _, val := range a {
			if slices.Contains(b, val) {
				return false
			}
		}
		return true
	}
	return false
}

func DNS1035(str string) string {
	re := regexp.MustCompile(`[^a-z0-9-]+`)
	return re.ReplaceAllString(strings.ToLower(str), "-")
}

// Hashcode returns a unique hashcode of a string.
func Hashcode(str string) int {
	i := int(crc32.ChecksumIEEE([]byte(str)))
	if i >= 0 {
		return i
	}
	if -i >= 0 {
		return -i
	}
	return 0
}
