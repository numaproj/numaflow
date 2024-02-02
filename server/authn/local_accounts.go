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

package authn

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/bcrypt"
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/server/common"
)

// LoginCredentials includes the user information
// It holds the username and password for the user
// used for local user authentication
type LoginCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Account holds local account information
type Account struct {
	InitialPasswordHash string
	PasswordHash        string
	Enabled             bool
}

// TODO: refactor to use a logger from server
var logger = logging.NewLogger().Named("accounts")

// GetAccount return an account info by the specified name.
func GetAccount(ctx context.Context, name string, kubeClient kubernetes.Interface) (*Account, error) {
	accounts, err := GetAccounts(ctx, kubeClient)
	if err != nil {
		return nil, fmt.Errorf("failed to get accounts for users:, %w", err)
	}
	account, ok := accounts[name]
	if !ok {
		return nil, fmt.Errorf("account: %v, does not exist", name)
	}
	return &account, nil
}

// GetAccounts returns list of configured accounts
func GetAccounts(ctx context.Context, kubeClient kubernetes.Interface) (map[string]Account, error) {
	var namespace = util.LookupEnvStringOr("NAMESPACE", "numaflow-system")
	secret, err := kubeClient.CoreV1().Secrets(namespace).Get(ctx, common.NumaflowAccountsSecret, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	cm, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, common.NumaflowAccountsConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return parseAccounts(secret, cm)
}

// parseAdminAccount parses the admin account from the secret and configMap
func parseAdminAccount(secret *k8sv1.Secret, cm *k8sv1.ConfigMap) (*Account, error) {
	adminAccount := &Account{Enabled: true}

	if adminInitialPasswordHash, ok := secret.Data[common.AdminInitialPasswordHashKey]; ok {
		adminAccount.InitialPasswordHash = string(adminInitialPasswordHash)
	}

	if adminPasswordHash, ok := secret.Data[common.AdminPasswordHashKey]; ok {
		adminAccount.PasswordHash = string(adminPasswordHash)
	}

	if enabledStr, ok := cm.Data[common.AdminEnabledKey]; ok {
		if enabled, err := strconv.ParseBool(enabledStr); err == nil {
			adminAccount.Enabled = enabled
		} else {
			return nil, fmt.Errorf("failed to parse configMap value for key %s: %w", common.AdminEnabledKey, err)
		}
	}

	return adminAccount, nil
}

// parseAccounts parses all the accounts from the secret and configMap
func parseAccounts(secret *k8sv1.Secret, cm *k8sv1.ConfigMap) (map[string]Account, error) {
	adminAccount, err := parseAdminAccount(secret, cm)
	if err != nil {
		return nil, err
	}

	accounts := map[string]Account{
		common.NumaflowAdminUsername: *adminAccount,
	}

	for key, v := range cm.Data {
		if !strings.HasSuffix(key, fmt.Sprintf(".%s", common.AccountUsernameSuffix)) {
			continue
		}

		val := v
		var accountName string

		parts := strings.Split(key, ".")
		if len(parts) == 2 {
			accountName = parts[0]
		} else {
			logger.Warnf("Unexpected key %s in ConfigMap '%s'", key, cm.Name)
			continue
		}

		account, ok := accounts[accountName]
		if !ok {
			account = Account{Enabled: true}
			accounts[accountName] = account
		}

		account.Enabled, err = strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("failed to parse configMap value for key %s: %w", key, err)
		}

		accounts[accountName] = account
	}

	for name, account := range accounts {
		if name == common.NumaflowAdminUsername {
			continue
		}

		if passwordHash, ok := secret.Data[fmt.Sprintf("%s.%s", name, common.AccountPasswordSuffix)]; ok {
			account.PasswordHash = string(passwordHash)
		}
		accounts[name] = account
	}

	return accounts, nil
}

// VerifyInitialPassword verifies the initial password for the admin user from the Accounts object
func VerifyInitialPassword(password, hashedPassword string) error {
	if password == hashedPassword {
		return nil
	}

	return fmt.Errorf("incorrect password enter for the user")
}

// VerifyPassword verifies the password(hashed) for the users from its respective Accounts object
func VerifyPassword(password, hashedPassword string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
}
