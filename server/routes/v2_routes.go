/*
Copyright 2026 The Numaproj Authors.

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

package routes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"

	v2 "github.com/numaproj/numaflow/server/apis/v2"
	"github.com/numaproj/numaflow/server/apis/v2/generated"
	"github.com/numaproj/numaflow/server/authz"
)

const (
	v2AuthzObjectExtension   = "x-numaflow-authz-object"
	v2RequiresAuthzExtension = "x-numaflow-requires-authz"
)

var openAPIPathParameter = regexp.MustCompile(`\{([^}]+)}`)

// registerV2Routes attaches generated OpenAPI routes and converts generated
// request-validation failures into the API v2 Problem response format.
func registerV2Routes(router gin.IRouter, handler generated.ServerInterface) {
	generated.RegisterHandlersWithOptions(router, handler, generated.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, _ int) {
			v2.WriteProblem(c, http.StatusUnprocessableEntity, "validation_failed", "Request validation failed", err.Error(), nil)
		},
	})
}

// V2AuthRouteMap derives the authorization metadata from the embedded OpenAPI
// contract so each declared v2 operation has one source of truth for its policy.
func V2AuthRouteMap(baseHref string) (authz.RouteMap, error) {
	spec, err := generated.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("load API v2 OpenAPI contract: %w", err)
	}
	return v2AuthRouteMap(spec, baseHref)
}

func v2AuthRouteMap(spec *openapi3.T, baseHref string) (authz.RouteMap, error) {
	if spec.Paths == nil {
		return nil, fmt.Errorf("API v2 OpenAPI contract has no paths")
	}
	result := authz.RouteMap{}
	for path, item := range spec.Paths.Map() {
		for method, operation := range item.Operations() {
			objectValue, present := operation.Extensions[v2AuthzObjectExtension]
			if !present {
				return nil, fmt.Errorf("%s %s is missing %s", method, path, v2AuthzObjectExtension)
			}
			object, ok := extensionValue[string](objectValue)
			if !ok || object == "" {
				return nil, fmt.Errorf("%s %s is missing %s", method, path, v2AuthzObjectExtension)
			}
			requiresAuthzValue, present := operation.Extensions[v2RequiresAuthzExtension]
			if !present {
				return nil, fmt.Errorf("%s %s is missing %s", method, path, v2RequiresAuthzExtension)
			}
			requiresAuthz, ok := extensionValue[bool](requiresAuthzValue)
			if !ok {
				return nil, fmt.Errorf("%s %s is missing %s", method, path, v2RequiresAuthzExtension)
			}
			ginPath := openAPIPathParameter.ReplaceAllString(path, ":$1")
			key := strings.ToUpper(method) + ":" + baseHref + "api/v2" + ginPath
			result[key] = authz.NewRouteInfo(object, requiresAuthz)
		}
	}
	return result, nil
}

// extensionValue decodes OpenAPI extension values because the loader may expose
// them as generic JSON values instead of the requested Go type.
func extensionValue[T any](value any) (T, bool) {
	var zero T
	if value == nil {
		return zero, false
	}
	if typed, ok := value.(T); ok {
		return typed, true
	}
	data, err := json.Marshal(value)
	if err != nil {
		return zero, false
	}
	if err = json.Unmarshal(data, &zero); err != nil {
		return zero, false
	}
	return zero, true
}
