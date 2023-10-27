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
	_ "embed"
	"fmt"
	"path"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/gin-gonic/gin"
)

var (
	//go:embed rbac-model.conf
	rbacModel string
)

const (
	emptyString = ""
)

type CasbinObject struct {
	enforcer *casbin.Enforcer
}

func NewCasbinObject() (*CasbinObject, error) {
	enforcer, err := getEnforcer()
	if err != nil {
		return nil, err
	}
	return &CasbinObject{
		enforcer: enforcer,
	}, nil
}

func (cas *CasbinObject) Authorize(c *gin.Context, groups []string) (bool, error) {
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	// Check if the user has permission for any of the groups.
	for _, group := range groups {
		// Get the user from the group. The group is in the format "group:role".
		// Check if the user has permission using Casbin Enforcer.
		if ok, _ := cas.enforcer.Enforce(group, resource, object, action); ok {
			return true, nil
		}
	}
	return false, fmt.Errorf("user is not authorized to execute the requested action")
}

// getEnforcer initializes the Casbin Enforcer with the model and policy.
func getEnforcer() (*casbin.Enforcer, error) {
	modelRBAC, err := model.NewModelFromString(rbacModel)
	if err != nil {
		return nil, err
	}
	a := fileadapter.NewAdapter(policyMapPath)

	// Initialize the Casbin Enforcer with the model and policies.
	enforcer, err := casbin.NewEnforcer(modelRBAC, a)
	if err != nil {
		return nil, err
	}
	enforcer.AddFunction("patternMatch", patternMatch)
	enforcer.AddFunction("stringMatch", stringMatch)
	return enforcer, nil
}

// patternMatch is used to match namespaces from the policy, if * is provided it will match all namespaces.
// Otherwise, we will enforce based on the namespace provided.
func patternMatch(args ...interface{}) (interface{}, error) {
	req, policy, err := extractArgs(args)
	if err != nil {
		return false, err
	}
	// If policy is MatchAll, allow all the strings.
	if policy == MatchAll {
		return true, nil
	}
	// pattern, namespace
	return path.Match(policy, req)
}

// stringMatch is used to match strings from the policy, if * is provided it will match all namespaces.
// Otherwise, we will enforce based on the namespace provided.
func stringMatch(args ...interface{}) (interface{}, error) {
	req, policy, err := extractArgs(args)
	if err != nil {
		return false, err
	}

	// If policy is MatchAll, allow all the strings.
	if policy == MatchAll {
		return true, nil
	}
	// pattern, namespace
	return policy == req, nil
}

// extractArgs extracts the arguments from the Casbin policy.
func extractArgs(args ...interface{}) (string, string, error) {
	// Should be 2 arguments as a list
	args = args[0].([]interface{})
	if len(args) != 2 {
		return emptyString, emptyString, fmt.Errorf("expected 2 arguments, got %d", len(args))
	}
	req, ok := args[0].(string)
	if !ok {
		return emptyString, emptyString, fmt.Errorf("expected first argument to be string, got %T", args[0])
	}
	policy, ok := args[1].(string)
	if !ok {
		return emptyString, emptyString, fmt.Errorf("expected second argument to be string, got %T", args[1])
	}
	return req, policy, nil
}

// extractResource extracts the resource from the request.
func extractResource(c *gin.Context) string {
	// We use the namespace in the request as the resource.
	resource := c.Param(ResourceNamespace)
	if resource == emptyString {
		return emptyString
	}
	return resource
}

// extractObject extracts the object from the request.
func extractObject(c *gin.Context) string {
	action := c.Request.Method
	// Get the route map from the context. Key is in the format "method:path".
	routeMapKey := fmt.Sprintf("%s:%s", action, c.FullPath())
	// Return the object from the route map.
	if RouteMap[routeMapKey] != nil {
		return RouteMap[routeMapKey].Object
	}
	return emptyString
}
