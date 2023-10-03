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

// CompareSlice compares two slices based on operator
func CompareSlice(operator v1alpha1.LogicOperator, sa []string, sb []string) bool {
	switch operator {
	case v1alpha1.LogicOperatorAnd:
		// returns true if all the elements of slice a are in slice b
		for _, val := range sa {
			if !StringSliceContains(sb, val) {
				return false
			}
		}
		return true

	case v1alpha1.LogicOperatorNot:
		// returns false if any of the elements of slice a are in slice b
		for _, val := range sa {
			if StringSliceContains(sb, val) {
				return false
			}
		}
		return true

	case v1alpha1.LogicOperatorOr:
		// returns true if any of the elements of slice a are in slice b
		for _, val := range sa {
			if StringSliceContains(sb, val) {
				return true
			}
		}
		return false
	}
	return false
}
