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

// RouteInfo is a struct which contains the route information with the object
// corresponding to the route and a boolean to indicate whether the route requires
// authorization.
type RouteInfo struct {
	Object        string
	RequiresAuthZ bool
}

// newRouteInfo creates a new RouteInfo object.
func newRouteInfo(object string, requiresAuthZ bool) *RouteInfo {
	return &RouteInfo{
		Object:        object,
		RequiresAuthZ: requiresAuthZ,
	}
}

// RouteMap is a map of routes to their corresponding RouteInfo objects.
// It saves the object corresponding to the route and a boolean to indicate
// whether the route requires authorization.
var RouteMap = map[string]*RouteInfo{
	"GET:/api/v1/sysinfo":                                                         newRouteInfo(ObjectPipeline, false),
	"GET:/api/v1/authinfo":                                                        newRouteInfo(ObjectEvents, false),
	"GET:/api/v1/namespaces":                                                      newRouteInfo(ObjectEvents, false),
	"GET:/api/v1/cluster-summary":                                                 newRouteInfo(ObjectPipeline, false),
	"GET:/api/v1/namespaces/:namespace/pipelines":                                 newRouteInfo(ObjectPipeline, true),
	"POST:/api/v1/namespaces/:namespace/pipelines":                                newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/pipelines/:pipeline":                       newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/pipelines/:pipeline/health":                newRouteInfo(ObjectPipeline, true),
	"PUT:/api/v1/namespaces/:namespace/pipelines/:pipeline":                       newRouteInfo(ObjectPipeline, true),
	"DELETE:/api/v1/namespaces/:namespace/pipelines/:pipeline":                    newRouteInfo(ObjectPipeline, true),
	"PATCH:/api/v1/namespaces/:namespace/pipelines/:pipeline":                     newRouteInfo(ObjectPipeline, true),
	"POST:/api/v1/namespaces/:namespace/isb-services":                             newRouteInfo(ObjectISBSvc, true),
	"GET:/api/v1/namespaces/:namespace/isb-services":                              newRouteInfo(ObjectISBSvc, true),
	"GET:/api/v1/namespaces/:namespace/isb-services/:isb-service":                 newRouteInfo(ObjectISBSvc, true),
	"PUT:/api/v1/namespaces/:namespace/isb-services/:isb-service":                 newRouteInfo(ObjectISBSvc, true),
	"DELETE:/api/v1/namespaces/:namespace/isb-services/:isb-service":              newRouteInfo(ObjectISBSvc, true),
	"GET:/api/v1/namespaces/:namespace/pipelines/:pipeline/isbs":                  newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/pipelines/:pipeline/watermarks":            newRouteInfo(ObjectPipeline, true),
	"PUT:/api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex":      newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/metrics":      newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods": newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/metrics/namespaces/:namespace/pods":                              newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/pods/:pod/logs":                            newRouteInfo(ObjectPipeline, true),
	"GET:/api/v1/namespaces/:namespace/events":                                    newRouteInfo(ObjectEvents, true),
}
