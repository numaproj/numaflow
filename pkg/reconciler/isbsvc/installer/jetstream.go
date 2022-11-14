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

package installer

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/shared/tls"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const (
	clientPort  = int32(4222)
	clusterPort = int32(6222)
	monitorPort = int32(8222)
	metricsPort = int32(7777)
)

var (
	//go:embed assets/jetstream/*
	jetStremAssets embed.FS
)

type jetStreamInstaller struct {
	client client.Client
	isbs   *dfv1.InterStepBufferService
	config *reconciler.GlobalConfig
	labels map[string]string
	logger *zap.SugaredLogger
}

func NewJetStreamInstaller(client client.Client, isbs *dfv1.InterStepBufferService, config *reconciler.GlobalConfig, labels map[string]string, logger *zap.SugaredLogger) Installer {
	return &jetStreamInstaller{
		client: client,
		isbs:   isbs,
		config: config,
		labels: labels,
		logger: logger.With("isbs", isbs.Name),
	}
}

func (r *jetStreamInstaller) Install(ctx context.Context) (*dfv1.BufferServiceConfig, error) {
	if js := r.isbs.Spec.JetStream; js == nil {
		return nil, fmt.Errorf("invalid jetstream isbs spec")
	}
	r.isbs.Status.SetType(dfv1.ISBSvcTypeJetStream)
	// merge
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(r.config.ISBSvc.JetStream.BufferConfig)); err != nil {
		return nil, fmt.Errorf("invalid jetstream buffer config in global configuration, %w", err)
	}
	if x := r.isbs.Spec.JetStream.BufferConfig; x != nil {
		if err := v.MergeConfig(bytes.NewBufferString(*x)); err != nil {
			return nil, fmt.Errorf("failed to merge customized buffer config, %w", err)
		}
	}
	b, err := yaml.Marshal(v.AllSettings())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged buffer config, %w", err)
	}

	if err := r.createSecrets(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream auth secrets", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("JetStreamAuthSecretsFailed", err.Error())
		return nil, err
	}
	if err := r.createConfigMap(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream ConfigMap", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("JetStreamConfigMapFailed", err.Error())
		return nil, err
	}
	if err := r.createService(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream Service", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("JetStreamServiceFailed", err.Error())
		return nil, err
	}
	if err := r.createStatefulSet(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream StatefulSet", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("JetStreamStatefulSetFailed", err.Error())
		return nil, err
	}
	r.isbs.Status.MarkDeployed()
	return &dfv1.BufferServiceConfig{
		JetStream: &dfv1.JetStreamConfig{
			URL: fmt.Sprintf("nats://%s.%s.svc.cluster.local:%s", generateJetStreamServiceName(r.isbs), r.isbs.Namespace, strconv.Itoa(int(clientPort))),
			Auth: &dfv1.NATSAuth{
				User: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: generateJetStreamClientAuthSecretName(r.isbs),
					},
					Key: dfv1.JetStreamClientAuthSecretUserKey,
				},
				Password: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: generateJetStreamClientAuthSecretName(r.isbs),
					},
					Key: dfv1.JetStreamClientAuthSecretPasswordKey,
				},
			},
			BufferConfig: string(b),
			TLSEnabled:   r.isbs.Spec.JetStream.TLS,
		},
	}, nil
}

func (r *jetStreamInstaller) createService(ctx context.Context) error {
	spec := r.isbs.Spec.JetStream.GetServiceSpec(dfv1.GetJetStreamServiceSpecReq{
		Labels:      r.labels,
		MetricsPort: metricsPort,
		ClusterPort: clusterPort,
		ClientPort:  clientPort,
		MonitorPort: monitorPort,
	})
	hash := sharedutil.MustHash(spec)
	obj := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateJetStreamServiceName(r.isbs),
			Labels:    r.labels,
			Annotations: map[string]string{
				dfv1.KeyHash: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbs.GetObjectMeta(), dfv1.ISBGroupVersionKind),
			},
		},
		Spec: spec,
	}
	old := &corev1.Service{}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), old); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.client.Create(ctx, obj); err != nil {
				return fmt.Errorf("failed to create jetstream service, err: %w", err)
			}
			r.logger.Info("Created jetstream service successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if jetstream service is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Spec = spec
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update jetstream service, err: %w", err)
		}
		r.logger.Info("Updated jetstream service successfully")
	}
	return nil
}

func (r *jetStreamInstaller) createStatefulSet(ctx context.Context) error {
	jsVersion, err := r.config.GetJetStreamVersion(r.isbs.Spec.JetStream.Version)
	if err != nil {
		return fmt.Errorf("failed to get jetstream version, err: %w", err)
	}
	spec := r.isbs.Spec.JetStream.GetStatefulSetSpec(dfv1.GetJetStreamStatefulSetSpecReq{
		ServiceName:                generateJetStreamServiceName(r.isbs),
		Labels:                     r.labels,
		NatsImage:                  jsVersion.NatsImage,
		MetricsExporterImage:       jsVersion.MetricsExporterImage,
		ConfigReloaderImage:        jsVersion.ConfigReloaderImage,
		ClusterPort:                clusterPort,
		MonitorPort:                monitorPort,
		ClientPort:                 clientPort,
		MetricsPort:                metricsPort,
		ServerAuthSecretName:       generateJetStreamServerSecretName(r.isbs),
		ServerEncryptionSecretName: generateJetStreamServerSecretName(r.isbs),
		ConfigMapName:              generateJetStreamConfigMapName(r.isbs),
		PvcNameIfNeeded:            generateJetStreamPVCName(r.isbs),
		StartCommand:               jsVersion.StartCommand,
	})
	hash := sharedutil.MustHash(spec)
	obj := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateJetStreamStatefulSetName(r.isbs),
			Labels:    r.labels,
			Annotations: map[string]string{
				dfv1.KeyHash: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbs.GetObjectMeta(), dfv1.ISBGroupVersionKind),
			},
		},
		Spec: spec,
	}
	old := &appv1.StatefulSet{}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), old); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.client.Create(ctx, obj); err != nil {
				return fmt.Errorf("failed to create jetstream statefulset, err: %w", err)
			}
			r.logger.Info("Created jetstream statefulset successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if jetstream statefulset is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Spec = spec
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update jetstream statefulset, err: %w", err)
		}
		r.logger.Info("Updated jetstream statefulset successfully")
	}
	return nil
}

func (r *jetStreamInstaller) createSecrets(ctx context.Context) error {
	oldServerObjExisting, oldClientObjExisting := true, true

	oldSObj := &corev1.Secret{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: r.isbs.Namespace, Name: generateJetStreamServerSecretName(r.isbs)}, oldSObj); err != nil {
		if apierrors.IsNotFound(err) {
			oldServerObjExisting = false
		} else {
			return fmt.Errorf("failed to check if nats server auth secret is existing, err: %w", err)
		}
	}

	oldCObj := &corev1.Secret{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: r.isbs.Namespace, Name: generateJetStreamClientAuthSecretName(r.isbs)}, oldCObj); err != nil {
		if apierrors.IsNotFound(err) {
			oldClientObjExisting = false
		} else {
			return fmt.Errorf("failed to check if nats client auth secret is existing, err: %w", err)
		}
	}

	if oldClientObjExisting && oldServerObjExisting { // Both existing, do nothing
		return nil
	}

	if oldClientObjExisting {
		if err := r.client.Delete(ctx, oldSObj); err != nil {
			return fmt.Errorf("failed to delete malformed nats server auth secret, err: %w", err)
		}
		r.logger.Infow("Deleted malformed nats server auth secret successfully")
	}

	if oldServerObjExisting {
		if err := r.client.Delete(ctx, oldCObj); err != nil {
			return fmt.Errorf("failed to delete malformed nats client auth secret, err: %w", err)
		}
		r.logger.Infow("Deleted malformed nats client auth secret successfully")
	}

	jsEncryptionKey := sharedutil.RandomString(12)
	jsUser := sharedutil.RandomString(8)
	jsPass := sharedutil.RandomString(16)
	sysPassword := sharedutil.RandomString(24)
	authTlsConfig := ""
	if r.isbs.Spec.JetStream.TLS {
		authTlsConfig = `tls {
  cert_file: "/etc/nats-config/server-cert.pem"
  key_file: "/etc/nats-config/server-key.pem"
  ca_file: "/etc/nats-config/ca-cert.pem"
}`
	}
	authTpl := template.Must(template.ParseFS(jetStremAssets, "assets/jetstream/server-auth.conf"))
	var authTplOutput bytes.Buffer
	if err := authTpl.Execute(&authTplOutput, struct {
		JetStreamUser     string
		JetStreamPassword string
		SysPassword       string
		TLSConfig         string
	}{
		JetStreamUser:     jsUser,
		JetStreamPassword: jsPass,
		SysPassword:       sysPassword,
		TLSConfig:         authTlsConfig,
	}); err != nil {
		return fmt.Errorf("failed to parse nats auth template, error: %w", err)
	}

	// Generate Certs no matter if TLS is enabled or not, so that toggling does not need to regenerate
	r.logger.Info("Generating Certs")
	certOrg := "io.numaproj"
	// Generate server Cert
	hosts := []string{
		fmt.Sprintf("%s.%s.svc.cluster.local", generateJetStreamServiceName(r.isbs), r.isbs.Namespace),
		fmt.Sprintf("%s.%s.svc", generateJetStreamServiceName(r.isbs), r.isbs.Namespace),
	}
	serverKeyPEM, serverCertPEM, caCertPEM, err := tls.CreateCerts(certOrg, hosts, time.Now().Add(10*365*24*time.Hour), true, false)
	if err != nil {
		return fmt.Errorf("failed to generate JetStream server CERT, %w", err)
	}
	// Generate cluster Cert
	clusterNodeHosts := []string{
		fmt.Sprintf("*.%s.%s.svc.cluster.local", generateJetStreamServiceName(r.isbs), r.isbs.Namespace),
		fmt.Sprintf("*.%s.%s.svc", generateJetStreamServiceName(r.isbs), r.isbs.Namespace),
	}
	clusterKeyPEM, clusterCertPEM, clusterCACertPEM, err := tls.CreateCerts(certOrg, clusterNodeHosts, time.Now().Add(10*365*24*time.Hour), true, true)
	if err != nil {
		return fmt.Errorf("failed to generate JetStream cluster CERT, %w", err)
	}

	serverObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateJetStreamServerSecretName(r.isbs),
			Labels:    r.labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbs.GetObjectMeta(), dfv1.ISBGroupVersionKind),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			dfv1.JetStreamServerSecretAuthKey:       authTplOutput.Bytes(),
			dfv1.JetStreamServerSecretEncryptionKey: []byte(jsEncryptionKey), //Always generate and store encryption key so that toggling does not need to regenerate
			dfv1.JetStreamServerPrivateKeyKey:       serverKeyPEM,
			dfv1.JetStreamServerCertKey:             serverCertPEM,
			dfv1.JetStreamServerCACertKey:           caCertPEM,
			dfv1.JetStreamClusterPrivateKeyKey:      clusterKeyPEM,
			dfv1.JetStreamClusterCertKey:            clusterCertPEM,
			dfv1.JetStreamClusterCACertKey:          clusterCACertPEM,
		},
	}

	clientAuthObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateJetStreamClientAuthSecretName(r.isbs),
			Labels:    r.labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbs.GetObjectMeta(), dfv1.ISBGroupVersionKind),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			dfv1.JetStreamClientAuthSecretUserKey:     []byte(jsUser),
			dfv1.JetStreamClientAuthSecretPasswordKey: []byte(jsPass),
		},
	}

	if err := r.client.Create(ctx, serverObj); err != nil {
		return fmt.Errorf("failed to create nats server secret, err: %w", err)
	}
	r.logger.Infow("Created nats server secret successfully")

	if err := r.client.Create(ctx, clientAuthObj); err != nil {
		return fmt.Errorf("failed to create nats client auth secret, err: %w", err)
	}
	r.logger.Infow("Created nats client auth secret successfully")
	return nil
}

func (r *jetStreamInstaller) createConfigMap(ctx context.Context) error {
	data := make(map[string]string)
	svcName := generateJetStreamServiceName(r.isbs)
	ssName := generateJetStreamStatefulSetName(r.isbs)
	replicas := r.isbs.Spec.JetStream.GetReplicas()
	if replicas < 3 {
		replicas = 3
	}
	routes := []string{}
	for j := 0; j < replicas; j++ {
		routes = append(routes, fmt.Sprintf("nats://%s-%s.%s.%s.svc.cluster.local:%s", ssName, strconv.Itoa(j), svcName, r.isbs.Namespace, strconv.Itoa(int(clusterPort))))
	}
	settings := r.config.ISBSvc.JetStream.Settings
	if x := r.isbs.Spec.JetStream.Settings; x != nil {
		settings = *x
	}
	encryptionSettings := ""
	if r.isbs.Spec.JetStream.Encryption {
		encryptionSettings = "key: $JS_KEY"
	}
	clusterTLSConfig := ""
	if r.isbs.Spec.JetStream.TLS {
		clusterTLSConfig = `
  tls {
    cert_file: "/etc/nats-config/cluster-server-cert.pem"
	key_file: "/etc/nats-config/cluster-server-key.pem"
	ca_file: "/etc/nats-config/cluster-ca-cert.pem"
  }
`
	}
	confTpl := template.Must(template.ParseFS(jetStremAssets, "assets/jetstream/nats.conf"))
	var confTplOutput bytes.Buffer
	if err := confTpl.Execute(&confTplOutput, struct {
		ClusterName        string
		MonitorPort        string
		ClusterPort        string
		ClientPort         string
		Routes             string
		Settings           string
		EncryptionSettings string
		TLSConfig          string
	}{
		ClusterName:        r.isbs.Name,
		MonitorPort:        strconv.Itoa(int(monitorPort)),
		ClusterPort:        strconv.Itoa(int(clusterPort)),
		ClientPort:         strconv.Itoa(int(clientPort)),
		Routes:             strings.Join(routes, ","),
		Settings:           settings,
		EncryptionSettings: encryptionSettings,
		TLSConfig:          clusterTLSConfig,
	}); err != nil {
		return fmt.Errorf("failed to parse nats config template, error: %w", err)
	}
	data[dfv1.JetStreamConfigMapKey] = confTplOutput.String()

	hash := sharedutil.MustHash(data)
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateJetStreamConfigMapName(r.isbs),
			Labels:    r.labels,
			Annotations: map[string]string{
				dfv1.KeyHash: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbs.GetObjectMeta(), dfv1.ISBGroupVersionKind),
			},
		},
		Data: data,
	}
	old := &corev1.ConfigMap{}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), old); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.client.Create(ctx, obj); err != nil {
				return fmt.Errorf("failed to create jetstream configmap, err: %w", err)
			}
			r.logger.Info("Created jetstream configmap successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if jetstream configmap is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Data = data
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update jetstream configmap, err: %w", err)
		}
		r.logger.Info("Updated jetstream configmap successfully")
	}
	return nil
}

func (r *jetStreamInstaller) Uninstall(ctx context.Context) error {
	return r.uninstallPVCs(ctx)
}

func (r *jetStreamInstaller) uninstallPVCs(ctx context.Context) error {
	// StatefulSet doesn't clean up PVC, needs to do it separately
	// https://github.com/kubernetes/kubernetes/issues/55045
	pvcs, err := r.getPVCs(ctx)
	if err != nil {
		r.logger.Errorw("Failed to get PVCs created by Nats statefulset when uninstalling", zap.Error(err))
		return err
	}
	for _, pvc := range pvcs {
		err = r.client.Delete(ctx, &pvc)
		if err != nil {
			r.logger.Errorw("Failed to delete pvc when uninstalling", zap.Any("pvcName", pvc.Name), zap.Error(err))
			return err
		}
		r.logger.Infow("PVC deleted", "pvcName", pvc.Name)
	}
	return nil
}

// get PVCs created by streaming statefulset
// they have same labels as the statefulset
func (r *jetStreamInstaller) getPVCs(ctx context.Context) ([]corev1.PersistentVolumeClaim, error) {
	pvcl := &corev1.PersistentVolumeClaimList{}
	err := r.client.List(ctx, pvcl, &client.ListOptions{
		Namespace:     r.isbs.Namespace,
		LabelSelector: labels.SelectorFromSet(r.labels),
	})
	if err != nil {
		return nil, err
	}
	return pvcl.Items, nil
}

func generateJetStreamServerSecretName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-server", isbs.Name)
}

func generateJetStreamClientAuthSecretName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-client-auth", isbs.Name)
}

func generateJetStreamServiceName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-svc", isbs.Name)
}

func generateJetStreamStatefulSetName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js", isbs.Name)
}

func generateJetStreamConfigMapName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-config", isbs.Name)
}

func generateJetStreamPVCName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-vol", isbs.Name)
}
