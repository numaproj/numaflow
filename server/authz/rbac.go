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
	"strings"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/server/authn"
)

var (
	//go:embed rbac-model.conf
	rbacModel     string
	logger        = logging.NewLogger()
	userPermCount map[string]int
)

const (
	emptyString = ""
)

type CasbinObject struct {
	enforcer      *casbin.Enforcer
	config        *viper.Viper
	currentScopes []string
	policyDefault string
}

func NewCasbinObject() (*CasbinObject, error) {
	enforcer, err := getEnforcer()
	if err != nil {
		return nil, err
	}
	configReader := viper.New()
	configReader.SetConfigFile(rbacPropertiesPath)
	err = configReader.ReadInConfig()
	currentScopes := getRBACScopes(configReader)
	if err != nil {
		return nil, err
	}
	// Set the default policy for authorization.
	policyDefault := getDefaultPolicy(configReader)

	return &CasbinObject{
		enforcer:      enforcer,
		config:        configReader,
		currentScopes: currentScopes,
		policyDefault: policyDefault,
	}, nil
}

// GetConfig returns the config file of the authorizer. We use a config file to store the policy params
func (cas *CasbinObject) GetConfig() *viper.Viper {
	return cas.config
}

// GetScopes returns the scopes for the authorizer.
func (cas *CasbinObject) GetScopes() []string {
	return cas.currentScopes
}

// setScopes sets the scopes for the authorizer.
func (cas *CasbinObject) setScopes(scopes []string) {
	cas.currentScopes = scopes
}

// Authorize checks if a user is authorized to access the resource.
// It returns true if the user is authorized, otherwise false.
// It also returns the policy count of the user. The policy count is used to check if there are any policies defined
// for the given user, if not we will allocate a default policy for the user.
func (cas *CasbinObject) Authorize(c *gin.Context, userIdentityToken *authn.UserInfo, scope string) (bool, int) {
	switch scope {
	case ScopeGroup:
		return enforceGroups(cas.enforcer, c, userIdentityToken)
	case ScopeEmail:
		return enforceEmail(cas.enforcer, c, userIdentityToken)
	case ScopeDefault:
		return enforceDefault(cas.enforcer, c, cas.policyDefault)
	default:
		logger.Errorw("invalid scope provided in config", "scope", scope)
	}
	return false, 0
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

// getRbacProperty is used to read the rbacPropertiesPath file path and extract the policy provided as argument,
func getRbacProperty(property string, config *viper.Viper) interface{} {
	val := config.Get(property)
	if val == nil {
		return emptyString
	}
	return val
}

// getRBACScopes returns the scopes from the rbac properties file. If no scopes are provided, it returns Group as the
// default scope. The scopes are used to determine the user identity token to be used for authorization.
// If the scope is group, the user identity token will be the groups assigned to the user from the authentication
// system. If the scope is email, the user identity token will be the email assigned to
// the user from the authentication.
// The scopes are provided as a comma separated list in the rbac properties file.
// Example: policy.scopes=groups,email
func getRBACScopes(config *viper.Viper) []string {
	scopes := getRbacProperty(RbacPropertyScopes, config)
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
func enforceGroups(enforcer *casbin.Enforcer, c *gin.Context, userIdentityToken *authn.UserInfo) (bool, int) {
	groups := userIdentityToken.IDTokenClaims.Groups
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	policyCount := 0
	// Check if the user has permission for any of the groups.
	for _, group := range groups {
		policyCount += getPermissionCount(enforcer, group)
		// Get the user from the group. The group is in the format "group:role".
		// Check if the user has permission using Casbin Enforcer.
		if enforceCheck(enforcer, group, resource, object, action) {
			return true, policyCount
		}
	}
	return false, policyCount
}

// enforceEmail checks if the email assigned to the user form the authentication system has permission based on the
// Casbin model and policy.
func enforceEmail(enforcer *casbin.Enforcer, c *gin.Context, userIdentityToken *authn.UserInfo) (bool, int) {
	email := userIdentityToken.IDTokenClaims.Email
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	policyCount := getPermissionCount(enforcer, email)
	// Check if the user has permission based on the email.
	return enforceCheck(enforcer, email, resource, object, action), policyCount
}

// enforceCheck checks if the user has permission based on the Casbin model and policy.
func enforceCheck(enforcer *casbin.Enforcer, user, resource, object, action string) bool {
	ok, _ := enforcer.Enforce(user, resource, object, action)
	return ok
}

// ConfigFileReload is used to reload the config file when it is changed. This is used to reload the policy without
// restarting the server. The config file is in the format of yaml.
func ConfigFileReload(e fsnotify.Event, authorizer *CasbinObject) {
	logger.Infow("RBAC conf file updated:", "fileName", e.Name)
	conf := authorizer.GetConfig()
	err := conf.ReadInConfig()
	if err != nil {
		return
	}
	// update the scopes
	currentScopes := getRBACScopes(conf)
	authorizer.setScopes(currentScopes)
	// update the default policy
	authorizer.policyDefault = getDefaultPolicy(conf)
	// clear the userPermCount cache
	userPermCount = make(map[string]int)
	logger.Infow("Auth Scopes Updated", "scopes", authorizer.GetScopes())
}

// getDefaultPolicy returns the default policy from the rbac properties file. The default policy is used when the
// requested resource is not present in the policy.
// The default policy is provided in the rbac properties file in the format "policy.default: value"
// Example: policy.default: deny
func getDefaultPolicy(config *viper.Viper) string {
	defaultPolicy := getRbacProperty(RbacPropertyDefaultPolicy, config)
	if defaultPolicy == emptyString {
		return emptyString
	}
	return defaultPolicy.(string)
}

// getPermissionCount returns the number of permissions for a user.
func getPermissionCount(enforcer *casbin.Enforcer, user string) int {
	// check if user exists in userPermCount
	if userPermCount == nil {
		userPermCount = make(map[string]int)
	}
	if userPermCount[user] != 0 {
		return userPermCount[user]
	}
	// get the permissions for the user
	cnt, err := enforcer.GetImplicitPermissionsForUser(user)
	if err != nil {
		logger.Errorw("Failed to get permissions for user", "user", user, "error", err)
		return 0
	}
	count := len(cnt)
	// store the count in userPermCount
	userPermCount[user] = count
	return count
}

// enforceDefault checks if the user has permission based on the Casbin model and policy.
func enforceDefault(enforcer *casbin.Enforcer, c *gin.Context, user string) (bool, int) {
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	policyCount := getPermissionCount(enforcer, user)
	// Check if the user has permission based on the email.
	return enforceCheck(enforcer, user, resource, object, action), policyCount
}
