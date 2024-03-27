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
	"fmt"
	"os"
	"strconv"
)

func LookupEnvStringOr(key, defaultValue string) string {
	if v, existing := os.LookupEnv(key); existing && v != "" {
		return v
	} else {
		return defaultValue
	}
}

func LookupEnvIntOr(key string, defaultValue int) int {
	if valStr, existing := os.LookupEnv(key); existing && valStr != "" {
		val, err := strconv.Atoi(valStr)
		if err != nil {
			panic(fmt.Errorf("invalid value for env variable %q, value %q", key, valStr))
		}
		return val
	} else {
		return defaultValue
	}
}

func LookupEnvBoolOr(key string, defaultValue bool) bool {
	if valStr, existing := os.LookupEnv(key); existing && valStr != "" {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			panic(fmt.Errorf("invalid value for env variable %q, value %q", key, valStr))
		}
		return val
	} else {
		return defaultValue
	}
}
