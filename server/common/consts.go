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

package common

const (
	AppClientID                 = "numaflow-server-app"
	StateCookieName             = "numaflow-oauthstate"
	StateCookieMaxAge           = 60 * 5
	UserIdentityCookieName      = "numaflow.token"
	UserIdentityCookieMaxAge    = 60 * 60 * 8 // 8 hours
	LoginCookieName             = "numaflow-login"
	NumaflowAdminUsername       = "admin"
	NumaflowAccountsSecret      = "numaflow-server-secrets"
	NumaflowAccountsConfigMap   = "numaflow-server-config"
	NumaflowAccountsNamespace   = "numaflow-system"
	NumaflowServerSecretKey     = "server.secretkey"
	TokenIssuer                 = "numaflow-server"
	AdminInitialPasswordHashKey = "admin.initial-password"
	AdminPasswordHashKey        = "admin.password"
	AdminEnabledKey             = "admin.enabled"
	AccountsKeyPrefix           = "accounts"
	AccountPasswordSuffix       = "password"
	JWTCookieName               = "jwt"
	JWTCookieMaxAge             = 60 * 60 * 8 // 8 hours
)
