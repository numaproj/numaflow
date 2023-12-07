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

package authz

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

// RouteInfo is a struct which contains the route information with the object
// corresponding to the route and a boolean to indicate whether the route requires
// authorization.
type RouteInfo struct {
	Object        string
	RequiresAuthZ bool
}

// NewRouteInfo creates a new RouteInfo object.
func NewRouteInfo(object string, requiresAuthZ bool) *RouteInfo {
	return &RouteInfo{
		Object:        object,
		RequiresAuthZ: requiresAuthZ,
	}
}

// RouteMap type is a map of routes to their corresponding RouteInfo objects.
// It saves the object corresponding to the route and a boolean to indicate
// whether the route requires authorization.
type RouteMap map[string]*RouteInfo

// GetRouteMapKey returns the key for the AuthRouteMap.
// The key is a combination of the HTTP method and the path.
// The format is "method:path".
// For example, "GET:/api/v1/namespaces", "POST:/api/v1/namespaces".
// This key is used to get the RouteInfo object from the AuthRouteMap.
func GetRouteMapKey(c *gin.Context) string {
	return fmt.Sprintf("%s:%s", c.Request.Method, c.FullPath())
}

// GetRouteFromContext returns the RouteInfo object from the AuthRouteMap based on the context.
func (r RouteMap) GetRouteFromContext(c *gin.Context) *RouteInfo {
	if routeEntry, ok := r[GetRouteMapKey(c)]; ok {
		return routeEntry
	}
	return nil
}
