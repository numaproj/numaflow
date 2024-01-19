## **Configuring AuthZ in Numaflow**

We utilize a role-based access control (RBAC) model to manage authorization in Numaflow. Along with this we use Casbin as the library for the implementation of these policies.

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
