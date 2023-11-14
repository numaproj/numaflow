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

package webhook

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/inflect"
	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	clientadmissionregistrationv1 "k8s.io/client-go/kubernetes/typed/admissionregistration/v1"

	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	commontls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/numaproj/numaflow/webhook/validator"
)

const (
	secretServerKey  = "server-key.pem"
	secretServerCert = "server-cert.pem"
	secretCACert     = "ca-cert.pem"

	certOrg = "io.numaproj"
)

// Webhook configuration
type Options struct {
	WebhookName     string
	ServiceName     string
	DeploymentName  string
	ClusterRoleName string
	SecretName      string
	Namespace       string
	Port            int
	ClientAuth      tls.ClientAuthType
}

// Controller for validation webhook
type AdmissionController struct {
	Client     kubernetes.Interface
	NumaClient v1alpha1.NumaflowV1alpha1Interface

	Options  Options
	Handlers map[schema.GroupVersionKind]runtime.Object

	Logger *zap.SugaredLogger
}

func (ac *AdmissionController) Run(ctx context.Context) error {
	logger := ac.Logger
	tlsConfig, caCert, err := ac.configureCerts(ctx, ac.Options.ClientAuth)
	if err != nil {
		logger.Errorw("Could not configure admission webhook certs", zap.Error(err))
	}
	server := &http.Server{
		Handler:   ac,
		Addr:      fmt.Sprintf(":%v", ac.Options.Port),
		TLSConfig: tlsConfig,
	}
	cl := ac.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	if err := ac.register(ctx, cl, caCert); err != nil {
		logger.Errorw("Failed to register webhook", zap.Error(err))
		return err
	}

	logger.Info("Successfully registered webhook")

	serverStartErrCh := make(chan struct{})
	go func() {
		if err := server.ListenAndServeTLS("", ""); err != nil {
			logger.Errorw("ListenAndServeTLS for admission webhook errored out", zap.Error(err))
			close(serverStartErrCh)
		}
	}()
	select {
	case <-ctx.Done():
		return server.Close()
	case <-serverStartErrCh:
		return fmt.Errorf("webhook server failed to start")
	}
}

// Register registers the validating admission webhook
func (ac *AdmissionController) register(ctx context.Context, client clientadmissionregistrationv1.ValidatingWebhookConfigurationInterface, caCert []byte) error {
	failurePolicy := admissionregistrationv1.Ignore

	var rules []admissionregistrationv1.RuleWithOperations
	for gvk := range ac.Handlers {
		plural := strings.ToLower(inflect.Pluralize(gvk.Kind))

		rules = append(rules, admissionregistrationv1.RuleWithOperations{
			Operations: []admissionregistrationv1.OperationType{
				admissionregistrationv1.Create,
				admissionregistrationv1.Update,
				admissionregistrationv1.Delete,
			},
			Rule: admissionregistrationv1.Rule{
				APIGroups:   []string{gvk.Group},
				APIVersions: []string{gvk.Version},
				Resources:   []string{plural},
			},
		})
	}

	sort.Slice(rules, func(i, j int) bool {
		lhs, rhs := rules[i], rules[j]
		if lhs.APIGroups[0] != rhs.APIGroups[0] {
			return lhs.APIGroups[0] < rhs.APIGroups[0]
		}
		if lhs.APIVersions[0] != rhs.APIVersions[0] {
			return lhs.APIVersions[0] < rhs.APIVersions[0]
		}
		return lhs.Resources[0] < rhs.Resources[0]
	})

	sideEffects := admissionregistrationv1.SideEffectClassNone

	port := int32(ac.Options.Port)
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: ac.Options.WebhookName,
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name:                    ac.Options.WebhookName,
			Rules:                   rules,
			SideEffects:             &sideEffects,
			AdmissionReviewVersions: []string{"v1", "v1beta1"},
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Namespace: ac.Options.Namespace,
					Name:      ac.Options.ServiceName,
					Port:      &port,
				},
				CABundle: caCert,
			},
			FailurePolicy: &failurePolicy,
		}},
	}
	clusterRole, err := ac.Client.RbacV1().ClusterRoles().Get(ctx, ac.Options.ClusterRoleName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch webhook cluster role, %w", err)
	}
	clusterRoleRef := metav1.NewControllerRef(clusterRole, rbacv1.SchemeGroupVersion.WithKind("ClusterRole"))
	webhook.OwnerReferences = append(webhook.OwnerReferences, *clusterRoleRef)

	_, err = client.Create(ctx, webhook, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create a webhook, %w", err)
		}
		ac.Logger.Info("Webhook already exists")
		configuredWebhook, err := client.Get(ctx, ac.Options.WebhookName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve webhook, %w", err)
		}
		if !reflect.DeepEqual(configuredWebhook.Webhooks, webhook.Webhooks) {
			ac.Logger.Info("Updating webhook")
			// Set the ResourceVersion as required by update.
			webhook.ObjectMeta.ResourceVersion = configuredWebhook.ObjectMeta.ResourceVersion
			if _, err := client.Update(ctx, webhook, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to update webhook, %w", err)
			}
		} else {
			ac.Logger.Info("Webhook is valid")
		}
	} else {
		ac.Logger.Info("Created a webhook")
	}
	return nil
}

// ServeHTTP implements the validating admission webhook
func (ac *AdmissionController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ac.Logger.Infof("Webhook ServeHTTP request=%#v", r)

	// content type validation
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		http.Error(w, "invalid Content-Type, want `application/json`", http.StatusUnsupportedMediaType)
		return
	}

	var review admissionv1.AdmissionReview
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, fmt.Sprintf("could not decode body: %v", err), http.StatusBadRequest)
		return
	}
	logger := ac.Logger.With("kind", fmt.Sprint(review.Request.Kind)).
		With("namespace", review.Request.Namespace).
		With("name", review.Request.Name).
		With("operation", fmt.Sprint(review.Request.Operation)).
		With("resource", fmt.Sprint(review.Request.Resource)).
		With("subResource", fmt.Sprint(review.Request.SubResource)).
		With("userInfo", fmt.Sprint(review.Request.UserInfo))

	reviewResponse := ac.admit(logging.WithLogger(r.Context(), logger), review.Request)
	response := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
	}
	if reviewResponse != nil {
		response.Response = reviewResponse
		response.Response.UID = review.Request.UID
	}

	logger.Infof("AdmissionReview for %s: %v/%v response=%v",
		review.Request.Kind, review.Request.Namespace, review.Request.Name, reviewResponse)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
		return
	}
}

func (ac *AdmissionController) admit(ctx context.Context, request *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	log := logging.FromContext(ctx)
	switch request.Operation {
	case admissionv1.Create, admissionv1.Update:
	default:
		log.Infof("Operation not interested: %v %v", request.Kind, request.Operation)
		return &admissionv1.AdmissionResponse{Allowed: true}
	}
	v, err := validator.GetValidator(ctx, ac.Client, ac.NumaClient, request.Kind, request.OldObject.Raw, request.Object.Raw)
	if err != nil {
		return validator.DeniedResponse("failed to get a validator: %v", err)
	}

	switch request.Operation {
	case admissionv1.Create:
		return v.ValidateCreate(ctx)
	case admissionv1.Update:
		return v.ValidateUpdate(ctx)
	default:
		return validator.AllowedResponse()
	}
}

// Generate cert secret
func (ac *AdmissionController) generateSecret(ctx context.Context) (*corev1.Secret, error) {
	hosts := []string{}
	hosts = append(hosts, fmt.Sprintf("%s.%s.svc.cluster.local", ac.Options.ServiceName, ac.Options.Namespace))
	hosts = append(hosts, fmt.Sprintf("%s.%s.svc", ac.Options.ServiceName, ac.Options.Namespace))
	serverKey, serverCert, caCert, err := commontls.CreateCerts(certOrg, hosts, time.Now().Add(10*365*24*time.Hour), true, false)
	if err != nil {
		return nil, err
	}
	deployment, err := ac.Client.AppsV1().Deployments(ac.Options.Namespace).Get(ctx, ac.Options.DeploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch webhook deployment, %w", err)
	}
	deploymentRef := metav1.NewControllerRef(deployment, appsv1.SchemeGroupVersion.WithKind("Deployment"))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ac.Options.SecretName,
			Namespace: ac.Options.Namespace,
		},
		Data: map[string][]byte{
			secretServerKey:  serverKey,
			secretServerCert: serverCert,
			secretCACert:     caCert,
		},
	}
	secret.OwnerReferences = append(secret.OwnerReferences, *deploymentRef)
	return secret, nil
}

// getOrGenerateKeyCertsFromSecret creates CERTs if not existing and store in a secret
func (ac *AdmissionController) getOrGenerateKeyCertsFromSecret(ctx context.Context) (serverKey, serverCert, caCert []byte, err error) {
	secret, err := ac.Client.CoreV1().Secrets(ac.Options.Namespace).Get(ctx, ac.Options.SecretName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, nil, nil, err
		}
		// No existing secret, creating one
		newSecret, err := ac.generateSecret(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		_, err = ac.Client.CoreV1().Secrets(newSecret.Namespace).Create(ctx, newSecret, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, nil, nil, err
		}
		// Something else might have created, try fetching it one more time
		secret, err = ac.Client.CoreV1().Secrets(ac.Options.Namespace).Get(ctx, ac.Options.SecretName, metav1.GetOptions{})
		if err != nil {
			return nil, nil, nil, err
		}
	}

	var ok bool
	if serverKey, ok = secret.Data[secretServerKey]; !ok {
		return nil, nil, nil, fmt.Errorf("server key missing")
	}
	if serverCert, ok = secret.Data[secretServerCert]; !ok {
		return nil, nil, nil, fmt.Errorf("server cert missing")
	}
	if caCert, ok = secret.Data[secretCACert]; !ok {
		return nil, nil, nil, fmt.Errorf("ca cert missing")
	}
	return serverKey, serverCert, caCert, nil
}

// GetAPIServerExtensionCACert gets the K8s aggregate apiserver
// client CA cert used by validator. This certificate is provided by
// kubernetes.
func (ac *AdmissionController) getAPIServerExtensionCACert(ctx context.Context) ([]byte, error) {
	const name = "extension-apiserver-authentication"
	c, err := ac.Client.CoreV1().ConfigMaps(metav1.NamespaceSystem).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	const caFileName = "requestheader-client-ca-file"
	pem, ok := c.Data[caFileName]
	if !ok {
		return nil, fmt.Errorf("cannot find %s in ConfigMap %s", caFileName, name)
	}
	return []byte(pem), nil
}

func (ac *AdmissionController) configureCerts(ctx context.Context, clientAuth tls.ClientAuthType) (*tls.Config, []byte, error) {
	var apiServerCACert []byte
	if clientAuth >= tls.VerifyClientCertIfGiven {
		var err error
		apiServerCACert, err = ac.getAPIServerExtensionCACert(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	serverKey, serverCert, caCert, err := ac.getOrGenerateKeyCertsFromSecret(ctx)
	if err != nil {
		return nil, nil, err
	}
	tlsConfig, err := makeTLSConfig(serverCert, serverKey, apiServerCACert, clientAuth)
	if err != nil {
		return nil, nil, err
	}
	return tlsConfig, caCert, nil
}

// makeTLSConfig makes a TLS configuration
func makeTLSConfig(serverCert, serverKey, caCert []byte, clientAuthType tls.ClientAuthType) (*tls.Config, error) {
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	cert, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   clientAuthType,
	}, nil
}
