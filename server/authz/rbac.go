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
	"bufio"
	_ "embed"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/server/authn"
)

var (
	//go:embed rbac-model.conf
	rbacModel      string
	logger         = logging.NewLogger().Named("server")
	policyRegex, _ = regexp.Compile(`policy\.[A-Za-z]+:(.*?)`)
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

func (cas *CasbinObject) Authorize(c *gin.Context, userIdentityToken *authn.UserInfo, scope string) bool {
	switch scope {
	case ScopeGroup:
		return enforceGroups(cas.enforcer, c, userIdentityToken)
	case ScopeEmail:
		return enforceEmail(cas.enforcer, c, userIdentityToken)
	default:
		logger.Errorw("invalid scope provided in config", "scope", scope)
	}
	return false
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

// getRbacProperty is used to read the RbacPropertiesPath file path and extract the policy provided as argument,
func getRbacProperty(property string) (interface{}, error) {
	// read from RbacPropertiesPath file path
	readFile, err := os.Open(RbacPropertiesPath)
	if err != nil {
		return nil, err
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileLines []string

	for fileScanner.Scan() {
		fileLines = append(fileLines, fileScanner.Text())
	}

	readFile.Close()
	for _, line := range fileLines {
		ok := policyRegex.MatchString(line)
		if ok {
			prop, val := parseProperty(line)
			if prop == property {
				return val, nil
			}
		}

	}
	return "", nil
}

// parseProperty parses the property from the rbac properties file.
// The property is in the format "property-name: value"
func parseProperty(line string) (string, string) {
	// line is in the format "property-name: value"
	branchArray := strings.SplitN(line, ":", 2)
	propertyName := strings.TrimSpace(branchArray[0])
	value := strings.TrimSpace(branchArray[1])
	return propertyName, value
}

// GetRbacScopes returns the scopes from the rbac properties file. If no scopes are provided, it returns Group as the
// default scope. The scopes are used to determine the user identity token to be used for authorization.
// If the scope is group, the user identity token will be the groups assigned to the user from the authentication
// system. If the scope is email, the user identity token will be the email assigned to
// the user from the authentication.
// The scopes are provided as a comma separated list in the rbac properties file.
// Example: policy.scopes=groups,email
func GetRbacScopes() []string {
	scopes, err := getRbacProperty(RbacPropertyScopes)
	if err != nil {
		logger.Errorw("error while getting scopes from rbac properties file", "error", err)
		return nil
	}
	var retList []string
	// If no scopes are provided, set Group as the default scope.
	if scopes == emptyString {
		retList = append(retList, ScopeGroup)
		return retList
	}
	scopes = strings.Split(scopes.(string), ",")
	for _, scope := range scopes.([]string) {
		scope = strings.TrimSpace(scope)
		retList = append(retList, scope)
	}

	return retList
}

// enforceGroups checks if the groups assigned to the user form the authentication system has permission based on the
// Casbin model and policy.
func enforceGroups(enforcer *casbin.Enforcer, c *gin.Context, userIdentityToken *authn.UserInfo) bool {
	groups := userIdentityToken.IDTokenClaims.Groups
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	// Check if the user has permission for any of the groups.
	for _, group := range groups {
		// Get the user from the group. The group is in the format "group:role".
		// Check if the user has permission using Casbin Enforcer.
		if enforceCheck(enforcer, group, resource, object, action) {
			return true
		}
	}
	return false
}

// enforceEmail checks if the email assigned to the user form the authentication system has permission based on the
// Casbin model and policy.
func enforceEmail(enforcer *casbin.Enforcer, c *gin.Context, userIdentityToken *authn.UserInfo) bool {
	email := userIdentityToken.IDTokenClaims.Email
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	// Check if the user has permission based on the email.
	return enforceCheck(enforcer, email, resource, object, action)

}

// enforceCheck checks if the user has permission based on the Casbin model and policy.
func enforceCheck(enforcer *casbin.Enforcer, user, resource, object, action string) bool {
	ok, _ := enforcer.Enforce(user, resource, object, action)
	return ok
}
