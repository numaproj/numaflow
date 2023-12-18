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
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strconv"
	"strings"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/server/common"
)

// Account holds local account information
type Account struct {
	InitialPasswordHash string
	PasswordHash        string
	Enabled             bool
}

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

func parseAccounts(secret *k8sv1.Secret, cm *k8sv1.ConfigMap) (map[string]Account, error) {
	adminAccount, err := parseAdminAccount(secret, cm)
	if err != nil {
		return nil, err
	}

	accounts := map[string]Account{
		common.NumaflowAdminUsername: *adminAccount,
	}

	for key, v := range cm.Data {
		if !strings.HasPrefix(key, fmt.Sprintf("%s.", common.AccountsKeyPrefix)) {
			continue
		}

		val := v
		var accountName, suffix string

		parts := strings.Split(key, ".")
		if len(parts) == 3 {
			accountName = parts[1]
			suffix = parts[2]
		} else {
			logger.Warnf("Unexpected key %s in ConfigMap '%s'", key, cm.Name)
			continue
		}

		account, ok := accounts[accountName]
		if !ok {
			account = Account{Enabled: true}
			accounts[accountName] = account
		}
		if suffix == "enabled" {
			account.Enabled, err = strconv.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("failed to parse configMap value for key %s: %w", key, err)
			}
		}
		accounts[accountName] = account
	}

	for name, account := range accounts {
		if name == common.NumaflowAdminUsername {
			continue
		}

		if passwordHash, ok := secret.Data[fmt.Sprintf("%s.%s.%s", common.AccountsKeyPrefix, name, common.AccountPasswordSuffix)]; ok {
			account.PasswordHash = string(passwordHash)
		}
		accounts[name] = account
	}

	return accounts, nil
}
