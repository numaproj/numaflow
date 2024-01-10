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

const (
	// PolicyMapPath is the path to the policy map.
	policyMapPath = "/etc/numaflow/rbac-policy.csv"

	// rbacPropertiesPath is the path to the rbac properties file. It includes configuraion for authorization like
	// scope, default policy etc.
	rbacPropertiesPath = "/etc/numaflow/rbac-conf.yaml"

	// Objects for the RBAC policy
	ObjectAll      = "*"
	ObjectPipeline = "pipeline"
	ObjectISBSvc   = "isbsvc"
	ObjectEvents   = "events"

	// Resouces for the RBAC policy
	ResourceAll       = "*"
	ResourceNamespace = "namespace"

	// MatchAll is a wildcard to match all patterns
	MatchAll = "*"

	// RbacProperties contain the different properties for RBAC configuration
	RbacPropertyScopes        = "policy.scopes"
	RbacPropertyDefaultPolicy = "policy.default"

	// Auth scopes supported
	ScopeGroup    = "groups"
	ScopeEmail    = "email"
	ScopeUsername = "username"
	ScopeDefault  = "default"
)
