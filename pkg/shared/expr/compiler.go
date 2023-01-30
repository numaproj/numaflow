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

package expr

import (
	"fmt"

	"github.com/antonmedv/expr"
)

// Compile uses the given input expression to evaluate input message and compile it to a string.
// See examples in compile_test.go
func Compile(expression string, msg []byte) (string, error) {
	msgMap := map[string]interface{}{
		root: string(msg),
	}
	env := getFuncMap(msgMap)
	program, err := expr.Compile(expression, expr.Env(env))
	if err != nil {
		return "", fmt.Errorf("unable to compile expression '%s': %s", expression, err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return "", fmt.Errorf("unable to execute compiled program %v", err)
	}
	return fmt.Sprintf("%v", result), nil
}
