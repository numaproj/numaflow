# Authentication

Numaflow UI server provides 2 approaches for authentication.

- [SSO with Dex](dex.md)
- [Local users](local-users.md)

There's also an option to disable `authentication/authorization` by setting `server.disable.auth: "true"` in the ConfigMap
1numaflow-cmd-params-config`, in this case, everybody has full access and privileges to any features of the UI (not recommended).
