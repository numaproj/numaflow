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
	result := authz.RouteMap{}
	for path, item := range spec.Paths {
		for method, operation := range operations(item) {
			object, ok := extensionValue[string](operation.Extensions[v2AuthzObjectExtension])
			if !ok || object == "" {
				return nil, fmt.Errorf("%s %s is missing %s", method, path, v2AuthzObjectExtension)
			}
			requiresAuthz, ok := extensionValue[bool](operation.Extensions[v2RequiresAuthzExtension])
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
	if typed, ok := value.(T); ok {
		return typed, true
	}
	var result T
	data, err := json.Marshal(value)
	if err != nil {
		return result, false
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return result, false
	}
	return result, true
}

// operations returns every HTTP operation declared for one OpenAPI path item.
func operations(item *openapi3.PathItem) map[string]*openapi3.Operation {
	result := map[string]*openapi3.Operation{}
	if item.Get != nil {
		result[http.MethodGet] = item.Get
	}
	if item.Post != nil {
		result[http.MethodPost] = item.Post
	}
	if item.Put != nil {
		result[http.MethodPut] = item.Put
	}
	if item.Patch != nil {
		result[http.MethodPatch] = item.Patch
	}
	if item.Delete != nil {
		result[http.MethodDelete] = item.Delete
	}
	return result
}
