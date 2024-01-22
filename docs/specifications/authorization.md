# UI Authorization

We utilize a role-based access control (RBAC) model to manage authorization in Numaflow. Along with this we utilize [Casbin](https://casbin.org/) as a library for the implementation of these policies.

## Permissions and Policies

The following model configuration is given to define the policies. The policy model is defined in the Casbin policy language.

```
[request_definition]
r = sub, res, obj, act

[policy_definition]
p = sub, res, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && patternMatch(r.res, p.res) && stringMatch(r.obj, p.obj) && stringMatch(r.act, p.act)
```

The policy model consists of the following sections:

- request_definition: The request definition section defines the request attributes. In our case, the request attributes are the user, resource, action, and object.
- policy_definition: The policy definition section defines the policy attributes. In our case, the policy attributes are the user, resource, action, and object.
- role_definition: The role definition section defines the role attributes. In our case, the role attributes are the user and role.
- policy_effect: The policy effect defines what action is to be taken on auth, In our case, the policy effect is allow.
- matchers: The matcher section defines the matching logic which decides whether is a given request matches any policy or not. These matches are done in order of the definition above and shortcircuit at the first failure. There are custom functions like patternMatch and stringMatch.
  - patternMatch: This function is used to match the resource with the policy resource using os path pattern matching along with adding support for wildcards for allowAll.
  - stringMatch: This function is used to match the object and action and uses a simple exact string match. This also supports wildcards for allowAll

The policy model for us follows the following structure for all policies defined and any requests made to th UI server:

- User: The user requesting access to a resource. This could be any identifier, such as a username, email address, or ID.
- Resource: The namespace in the cluster which is being accessed by the user. This can allow for selective access to namespaces. We have wildcard "\*" to allow access to all namespaces.
- Object : This could be a specific resource in the namespace, such as a pipeline, isbsvc or any event based resource. We have wildcard "\*" to allow access to all resources.
- Action: The action being performed on the resource using the API. These follow the standard HTTP verbs, such as GET, POST, PUT, DELETE, etc. We have wildcard "\*" to allow access to all actions.

Refer to the [RBAC](../operations/ui/authz/rbac.md) to learn more about how to configure authorization policies for Numaflow UI.
