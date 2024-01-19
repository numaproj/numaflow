## Numaflow AuthZ User Guide

## **Authorization**

Authorization is the process of granting or denying access to specific resources, functionalities, or data within a system. It involves determining what actions or operations an authenticated user or system is permitted to perform based on their identity, role, or other attributes. Authorization ensures that users have the appropriate permissions to access and interact with certain features or information within a software application, service, or system.

Authorization is a crucial component of overall security, contributing to data protection, regulatory compliance, and the prevention of unauthorized access or misuse of resources.

### Authorization in Numaflow

In the context of Numaflow, a platform for managing pipelines, authorization plays a crucial role due to the dynamic nature of user operations.

It's a fundamental enabler for effective pipeline management, providing users with the right access for their tasks while safeguarding critical processes. It promotes collaboration, operational efficiency, and mitigates risks by limiting access based on user roles.

We utilize a role-based access control (RBAC) model to manage authorization in Numaflow. Along with this we utilize Casbin as a library for the implementation of these policies.

### Role-Based Access Control (RBAC)

Role-Based Access Control (RBAC) is an access control paradigm widely used to simplify and manage permissions within a system. In RBAC:

- Roles: Users are assigned roles, representing specific job functions or responsibilities.
- Permissions: Roles are associated with permissions, defining actions or operations users can perform.
- Users: Access permissions are determined by the roles assigned to users.
- Groups: RBAC often supports a hierarchical structure, allowing roles to inherit permissions from others.

### Casbin

Casbin is an open-source access control library designed for simplified and flexible authorization in applications. Key features of Casbin include policy-based authorization, support for multiple access control models (such as RBAC and ABAC), persistence options for storing policies, and middleware for easy integration with various web frameworks. Casbin facilitates the management of access control by providing a policy language to express rules and enforce them within applications. It is known for its adaptability, supporting different authorization models and backends, making it suitable for a wide range of applications.

### Permissions and Policies

We use the given policy model conf to define our policies. The policy model is defined in the Casbin policy language.

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

## **Configuring AuthZ in Numaflow**

The current RBAC policy and permissions are defined in the file [numaflow-server-rbac-config.yaml](https://github.com/numaproj/numaflow/blob/main/config/base/numaflow-server/numaflow-server-rbac-config.yaml). This file is loaded into the Casbin enforcer and used to enforce the policies.

There are two main sections in the file:

1. **Rules section**:

Policies and groups are the two main entities defined in this section.
Both of them work in conjunction with each other. The groups are used to define a set of users with the same permissions and the policies are used to define the specific permissions for these users or groups. 


```
# Policies go here
p, role:admin, *, *, *
p, role:readonly, *, *, GET
# Groups go here
# g, admin, role:admin
# g, my-github-org:my-github-team, role:readonly
```
Here we have defined two policies for the custom groups `role:admin` and `role:readonly`. 
- The first policy allows the group role:admin to access all resources in all namespaces with all actions.
- The second policy allows the group role:readonly to access all resources in all namespaces with the GET action.

To add a new **policy**, add a new line in the format:

```
p, <user/group>, <namespace>, <resource>, <action>
```

The namespace, resource and action can be replaced with "\*" to allow access to all namespaces, resources and actions respectively.

Few examples:

- a policy line `p, test@test.com, *, *, POST` would allow the user with the given email address to access all resources in all namespaces with the POST action.
- a policy line `p, test_user, *, *, *` would allow the user with the given username to access all resources in all namespaces with all actions.
- a policy line `p, role:admin_ns, test_ns, *, *` would allow the group role:admin_ns to access all resources in the namespace test_ns with all actions.
- a policy line `p, test_user, test_ns, *, GET` would allow the user with the given username to access all resources in the namespace test_ns with the GET action.

**Groups** can be defined by adding a new line in the format:

```
g, <user>, <group>
```

These are useful for defining a set of users with the same permissions. The group can be used in the policy definition in place of the user. And thus any user added to the group will have the same permissions as the group.

Few examples:

- a group line `g, test@test.com, role:readonly` would add the user with the given email address to the group role:readonly.
- a group line `g, test_user, role:admin` would add the user with the given username to the group role:admin.

2. **Configuration**: This defines certain properties for the Casbin enforcer. The properties are defined in the following format:

```
  rbac-conf.yaml: |
    policy.default: role:readonly
    policy.scopes: groups,email,username
```

We see two properties defined here:

- **policy.default**: This defines the default role for a user. If a user does not have any roles defined, then this role will be used for the user. This is useful for defining a default role for all users.
- **policy.scopes**: The scopes field controls which authentication scopes to examine during rbac enforcement. We can have multiple scopes, and the first scope that matches with the policy will be used.
  - "groups", which means that the groups field of the user's token will be examined, This is **default** value and is used if no scopes are defined.
  - "email", which means that the email field of the user's token will be examined
  - "username", which means that the username field of the user's token will be examined

Multiple scopes can be provided as a comma-separated, e.g `"groups,email,username"`

This scope information is used to extract the user information from the token and then used to enforce the policies. Thus is it important to have the rules defined in the above section to map with the scopes expected in the configuration.


**Note**: The rbac-conf.yaml file can be updated during runtime and the changes will be reflected immediately. This is useful for changing the default role for all users or adding a new scope to be used for rbac enforcement.

### Additional Resources

- [Casbin Documentation](https://casbin.org/)
