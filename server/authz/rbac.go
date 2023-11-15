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
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"k8s.io/utils/strings/slices"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/server/authn"
)

var (
	//go:embed rbac-model.conf
	rbacModel string
	logger    = logging.NewLogger()
)

const (
	emptyString = ""
)

// CasbinObject is the struct that implements the Authorizer interface.
// It contains the Casbin Enforcer, the current scopes, the default policy and the config reader.
// The config reader is used to watch for changes in the config file.
// The Casbin Enforcer is used to enforce the authorization policy.
// The current scopes are used to determine the user identity token to be used for authorization.
// policyDefault is the default policy to be used when the requested resource is not present in the policy.
// userPermCount is a cache to store the count of permissions for a user. If the user has permissions in the
// policy, we store the count in the cache and return based on the value.
type CasbinObject struct {
	enforcer      *casbin.Enforcer
	userPermCount *sync.Map
	currentScopes []string
	policyDefault string
	configReader  *viper.Viper
	opts          *options
	rwMutex       sync.RWMutex
}

// NewCasbinObject returns a new CasbinObject. It initializes the Casbin Enforcer with the model and policy.
// It also initializes the config reader to watch for changes in the config file.
func NewCasbinObject(inputOptions ...Option) (*CasbinObject, error) {
	// Set the default options.
	var opts = DefaultOptions()
	// Apply the input options.
	for _, inputOption := range inputOptions {
		inputOption(opts)
	}
	enforcer, err := getEnforcer(opts.policyMapPath)
	if err != nil {
		return nil, err
	}
	configReader := viper.New()
	configReader.SetConfigFile(opts.rbacPropertiesPath)
	err = configReader.ReadInConfig()
	if err != nil {
		return nil, err
	}
	currentScopes := getRBACScopes(configReader)
	logger.Infow("Auth Scopes", "scopes", currentScopes)
	// Set the default policy for authorization.
	policyDefault := getDefaultPolicy(configReader)

	cas := &CasbinObject{
		enforcer:      enforcer,
		userPermCount: &sync.Map{},
		currentScopes: currentScopes,
		policyDefault: policyDefault,
		configReader:  configReader,
		opts:          opts,
	}

	// Watch for changes in the config file.
	configReader.WatchConfig()
	configReader.OnConfigChange(func(in fsnotify.Event) {
		cas.configFileReload(in)
	})

	return cas, nil
}

// Authorize checks if a user is authorized to access the resource.
// It returns true if the user is authorized, otherwise false.
// It also returns the policy count of the user. The policy count is used to check if there are any policies defined
// for the given user, if not we will allocate a default policy for the user.
func (cas *CasbinObject) Authorize(c *gin.Context, userInfo *authn.UserInfo) bool {
	// Get the scopes to check from the policy.
	currentScopes := cas.getCurrentScopes()
	scopedList := getSubjectFromScope(currentScopes, userInfo)
	// Get the resource, object and action from the request.
	resource := extractResource(c)
	object := extractObject(c)
	action := c.Request.Method
	userHasPolicies := false
	// Check for the given scoped list if the user is authorized using any of the subjects in the list.
	for _, scopedSubject := range scopedList {
		// Check if the user has permissions in the policy for the given scoped subject.
		userHasPolicies = userHasPolicies || cas.hasPermissionsDefined(scopedSubject)
		if ok := enforceCheck(cas.enforcer, scopedSubject, resource, object, action); ok {
			return ok
		}
	}
	// If the user does not have any policy defined, allocate a default policy for the user.
	if !userHasPolicies {
		defaultPolicy := cas.getDefaultPolicy()
		logger.Debugw("No policy defined for the user, allocating default policy",
			"DefaultPolicy", defaultPolicy)
		ok := enforceCheck(cas.enforcer, defaultPolicy, resource, object, action)
		if ok {
			return ok
		}
	}
	return false
}

// getSubjectFromScope returns the subjects in the request for the given scopes.
// The scopes are the params used to check the authentication params to check if the
// user is authorized to access the resource. For any new scope, add the scope to the
// rbac properties file and add the scope to the cases below.
func getSubjectFromScope(scopes []string, userInfo *authn.UserInfo) []string {
	var scopedList []string
	// If the scope is group, fetch the groups from the user identity token.
	if slices.Contains(scopes, ScopeGroup) {
		scopedList = append(scopedList, userInfo.IDTokenClaims.Groups...)
	}
	// If the scope is email, fetch the email from the user identity token.
	if slices.Contains(scopes, ScopeEmail) {
		scopedList = append(scopedList, userInfo.IDTokenClaims.Email)
	}
	return scopedList
}

// getEnforcer initializes the Casbin Enforcer with the model and policy.
func getEnforcer(policyFilePath string) (*casbin.Enforcer, error) {
	modelRBAC, err := model.NewModelFromString(rbacModel)
	if err != nil {
		return nil, err
	}
	a := fileadapter.NewAdapter(policyFilePath)

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
	// match as pattern
	return path.Match(policy, req)
}

// stringMatch is used to match strings from the policy, if * is provided it will match all namespaces.
// Otherwise, we will enforce based on the string provided.
func stringMatch(args ...interface{}) (interface{}, error) {
	req, policy, err := extractArgs(args)
	if err != nil {
		return false, err
	}

	// If policy is MatchAll, allow all the strings.
	if policy == MatchAll {
		return true, nil
	}
	// match exact string
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

// getRbacProperty is used to read the RBAC property file path and extract the policy provided as argument,
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

// enforceCheck checks if the user has permission based on the Casbin model and policy.
// It returns true if the user is authorized, otherwise false.
func enforceCheck(enforcer *casbin.Enforcer, user, resource, object, action string) bool {
	ok, _ := enforcer.Enforce(user, resource, object, action)
	return ok
}

// configFileReload is used to reload the config file when it is changed. This is used to reload the policy without
// restarting the server. The config file is in the format of yaml. The config file is read by viper.
func (cas *CasbinObject) configFileReload(e fsnotify.Event) {
	logger.Infow("RBAC conf file updated:", "fileName", e.Name)
	cas.rwMutex.RLock()
	err := cas.configReader.ReadInConfig()
	if err != nil {
		cas.rwMutex.RUnlock()
		return
	}
	cas.rwMutex.RUnlock()
	// update the scopes
	newScopes := getRBACScopes(cas.configReader)
	cas.setCurrentScopes(newScopes)
	// update the default policy
	policyDefault := getDefaultPolicy(cas.configReader)
	cas.setDefaultPolicy(policyDefault)
	// clear the userPermCount cache
	cas.userPermCount = &sync.Map{}

	logger.Infow("Auth Scopes Updated", "scopes", cas.getCurrentScopes())
}

// getDefaultPolicy returns the default policy from the rbac properties file. The default policy is used when the
// requested resource is not present in the policy.
// The default policy is provided in the rbac properties file in the format "policy.default: value"
// Example: policy.default: deny
func getDefaultPolicy(config *viper.Viper) string {
	defaultPolicy := getRbacProperty(RbacPropertyDefaultPolicy, config)
	return defaultPolicy.(string)
}

// hasPermissionsDefined checks if the user has permissions defined in the policy. It returns true if the user has
// permissions in the policy and false if the user does not have permissions in the policy.
// We have a cache userPermCount to store the count of permissions for a user. If the user has permissions in the
// policy, we store the count in the cache and return based on the value.
// If the user does not have permissions in the policy, we add it to the cache before returning
func (cas *CasbinObject) hasPermissionsDefined(user string) bool {
	// check if user exists in userPermCount
	val, ok := cas.userPermCount.Load(user)
	// If the key exists
	if ok {
		// Return true if the user has permissions in the policy
		// and false if the user does not have permissions in the policy.
		return val.(bool)
	}
	// get the permissions for the user
	cnt, err := cas.enforcer.GetImplicitPermissionsForUser(user)
	if err != nil {
		logger.Errorw("Failed to get permissions for user", "user", user, "error", err)
		return false
	}
	// If the user has permissions in the policy, len(cnt) > 0
	hasPerms := len(cnt) > 0
	// store the count in userPermCount
	cas.userPermCount.Store(user, hasPerms)
	return hasPerms
}

// setCurrentScopes sets the current scopes to the given scopes.
// It is used to update the scopes when the config file is changed.
// It is thread safe.
func (cas *CasbinObject) setCurrentScopes(scopes []string) {
	cas.rwMutex.Lock()
	defer cas.rwMutex.Unlock()
	cas.currentScopes = scopes
}

// getCurrentScopes returns the current scopes from the CasbinObject.
// It is thread safe.
func (cas *CasbinObject) getCurrentScopes() []string {
	cas.rwMutex.RLock()
	defer cas.rwMutex.RUnlock()
	return cas.currentScopes
}

// setDefaultPolicy sets the default policy to the given policy.
// It is used to update the default policy when the config file is changed.
// It is thread safe.
func (cas *CasbinObject) setDefaultPolicy(policy string) {
	cas.rwMutex.Lock()
	defer cas.rwMutex.Unlock()
	cas.policyDefault = policy
}

// getDefaultPolicy returns the default policy from the CasbinObject.
// It is thread safe.
func (cas *CasbinObject) getDefaultPolicy() string {
	cas.rwMutex.RLock()
	defer cas.rwMutex.RUnlock()
	return cas.policyDefault
}
