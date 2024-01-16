# Authentication

Numaflow UI server provides 2 approaches for authentication.

- Local users
- SSO with [Dex](https://github.com/dexidp/dex)

You also have the option to disable `authN/authZ` by setting `server.disable.auth: "true"` in the ConfigMap `numaflow-cmd-params-config`, in this case, everybody has full access and privileges to any features of the UI (not recommended).
