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

	"github.com/spf13/viper"
	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/shared/tls"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
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
	client     client.Client
	kubeClient kubernetes.Interface
	isbSvc     *dfv1.InterStepBufferService
	config     *reconciler.GlobalConfig
	labels     map[string]string
	logger     *zap.SugaredLogger
	recorder   record.EventRecorder
}

func NewJetStreamInstaller(client client.Client, kubeClient kubernetes.Interface, isbSvc *dfv1.InterStepBufferService, config *reconciler.GlobalConfig, labels map[string]string, logger *zap.SugaredLogger, recorder record.EventRecorder) Installer {
	return &jetStreamInstaller{
		client:     client,
		kubeClient: kubeClient,
		isbSvc:     isbSvc,
		config:     config,
		labels:     labels,
		logger:     logger.With("isbsvc", isbSvc.Name),
		recorder:   recorder,
	}
}

func (r *jetStreamInstaller) Install(ctx context.Context) (*dfv1.BufferServiceConfig, error) {
	if js := r.isbSvc.Spec.JetStream; js == nil {
		return nil, fmt.Errorf("invalid jetstream ISB Service spec")
	}
	r.isbSvc.Status.SetType(dfv1.ISBSvcTypeJetStream)
	// merge
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(r.config.GetISBSvcConfig().JetStream.BufferConfig)); err != nil {
		return nil, fmt.Errorf("invalid jetstream buffer config in global configuration, %w", err)
	}
	if x := r.isbSvc.Spec.JetStream.BufferConfig; x != nil {
		if err := v.MergeConfig(bytes.NewBufferString(*x)); err != nil {
			return nil, fmt.Errorf("failed to merge customized buffer config, %w", err)
		}
	}
	if r.isbSvc.Spec.JetStream.GetReplicas() < 3 {
		// Replica can not > 1 with non-cluster mode
		v.Set("otbucket.replicas", 1)
		v.Set("procbucket.replicas", 1)
		v.Set("stream.replicas", 1)
	}
	b, err := yaml.Marshal(v.AllSettings())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged buffer config, %w", err)
	}

	if err := r.createSecrets(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream auth secrets", zap.Error(err))
		r.isbSvc.Status.MarkDeployFailed("JetStreamAuthSecretsFailed", err.Error())
		r.recorder.Eventf(r.isbSvc, corev1.EventTypeWarning, "JetStreamAuthSecretsFailed", "Failed to create jetstream auth secrets: %v", err.Error())
		return nil, err
	}
	if err := r.createConfigMap(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream ConfigMap", zap.Error(err))
		r.isbSvc.Status.MarkDeployFailed("JetStreamConfigMapFailed", err.Error())
		r.recorder.Eventf(r.isbSvc, corev1.EventTypeWarning, "JetStreamConfigMapFailed", "Failed to create jetstream ConfigMap: %v", err.Error())
		return nil, err
	}
	if err := r.createService(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream Service", zap.Error(err))
		r.isbSvc.Status.MarkDeployFailed("JetStreamServiceFailed", err.Error())
		r.recorder.Eventf(r.isbSvc, corev1.EventTypeWarning, "JetStreamServiceFailed", "Failed to create jetstream Service: %v", err.Error())
		return nil, err
	}
	if err := r.createStatefulSet(ctx); err != nil {
		r.logger.Errorw("Failed to create jetstream StatefulSet", zap.Error(err))
		r.isbSvc.Status.MarkDeployFailed("JetStreamStatefulSetFailed", err.Error())
		r.recorder.Eventf(r.isbSvc, corev1.EventTypeWarning, "JetStreamStatefulSetFailed", "Failed to create jetstream StatefulSet: %v", err.Error())
		return nil, err
	}
	r.isbSvc.Status.MarkDeployed()
	reconciler.JetStreamISBSvcReplicas.WithLabelValues(r.isbSvc.Namespace, r.isbSvc.Name).Set(float64(r.isbSvc.Spec.JetStream.GetReplicas()))
	return &dfv1.BufferServiceConfig{
		JetStream: &dfv1.JetStreamConfig{
			URL: fmt.Sprintf("nats://%s.%s.svc:%s", generateJetStreamServiceName(r.isbSvc), r.isbSvc.Namespace, strconv.Itoa(int(clientPort))),
			Auth: &dfv1.NatsAuth{
				Basic: &dfv1.BasicAuth{
					User: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: generateJetStreamClientAuthSecretName(r.isbSvc),
						},
						Key: dfv1.JetStreamClientAuthSecretUserKey,
					},
					Password: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: generateJetStreamClientAuthSecretName(r.isbSvc),
						},
						Key: dfv1.JetStreamClientAuthSecretPasswordKey,
					},
				},
			},
			StreamConfig: string(b),
			TLSEnabled:   r.isbSvc.Spec.JetStream.TLS,
		},
	}, nil
}

func (r *jetStreamInstaller) createService(ctx context.Context) error {
	spec := r.isbSvc.Spec.JetStream.GetServiceSpec(dfv1.GetJetStreamServiceSpecReq{
		Labels:      r.labels,
		MetricsPort: metricsPort,
		ClusterPort: clusterPort,
		ClientPort:  clientPort,
		MonitorPort: monitorPort,
	})
	hash := sharedutil.MustHash(spec)
	obj := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbSvc.Namespace,
			Name:      generateJetStreamServiceName(r.isbSvc),
			Labels:    r.labels,
			Annotations: map[string]string{
				dfv1.KeyHash: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbSvc.GetObjectMeta(), dfv1.ISBGroupVersionKind),
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
			r.recorder.Event(r.isbSvc, corev1.EventTypeNormal, "JetStreamServiceSuccess", "Created jetstream service successfully")
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
		r.recorder.Event(r.isbSvc, corev1.EventTypeNormal, "JetStreamServiceSuccess", "Updated jetstream service successfully")
	}
	return nil
}

func (r *jetStreamInstaller) createStatefulSet(ctx context.Context) error {
	jsVersion, err := r.config.GetISBSvcConfig().GetJetStreamVersion(r.isbSvc.Spec.JetStream.Version)
	if err != nil {
		return fmt.Errorf("failed to get jetstream version, err: %w", err)
	}

	spec := r.isbSvc.Spec.JetStream.GetStatefulSetSpec(dfv1.GetJetStreamStatefulSetSpecReq{
		ServiceName:                generateJetStreamServiceName(r.isbSvc),
		Labels:                     r.labels,
		NatsImage:                  jsVersion.NatsImage,
		MetricsExporterImage:       jsVersion.MetricsExporterImage,
		ConfigReloaderImage:        jsVersion.ConfigReloaderImage,
		ClusterPort:                clusterPort,
		MonitorPort:                monitorPort,
		ClientPort:                 clientPort,
		MetricsPort:                metricsPort,
		ServerAuthSecretName:       generateJetStreamServerSecretName(r.isbSvc),
		ServerEncryptionSecretName: generateJetStreamServerSecretName(r.isbSvc),
		ConfigMapName:              generateJetStreamConfigMapName(r.isbSvc),
		PvcNameIfNeeded:            generateJetStreamPVCName(r.isbSvc),
		StartCommand:               jsVersion.StartCommand,
		DefaultResources:           r.config.GetDefaults().GetDefaultContainerResources(),
	})
	hash := sharedutil.MustHash(spec)
	obj := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbSvc.Namespace,
			Name:      generateJetStreamStatefulSetName(r.isbSvc),
			Labels:    r.labels,
			Annotations: map[string]string{
				dfv1.KeyHash: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbSvc.GetObjectMeta(), dfv1.ISBGroupVersionKind),
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
			r.recorder.Event(r.isbSvc, corev1.EventTypeNormal, "JetStreamStatefulSetSuccess", "Created jetstream stateful successfully")
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
		r.recorder.Event(r.isbSvc, corev1.EventTypeNormal, "JetStreamStatefulSet", "Updated jetstream statefulset successfully")
	}
	return nil
}

func (r *jetStreamInstaller) createSecrets(ctx context.Context) error {
	oldServerObjExisting, oldClientObjExisting := true, true

	oldSObj, err := r.kubeClient.CoreV1().Secrets(r.isbSvc.Namespace).Get(ctx, generateJetStreamServerSecretName(r.isbSvc), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			oldServerObjExisting = false
		} else {
			return fmt.Errorf("failed to check if nats server auth secret is existing, err: %w", err)
		}
	}

	oldCObj, err := r.kubeClient.CoreV1().Secrets(r.isbSvc.Namespace).Get(ctx, generateJetStreamClientAuthSecretName(r.isbSvc), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			oldClientObjExisting = false
		} else {
			return fmt.Errorf("failed to check if nats client auth secret is existing, err: %w", err)
		}
	}

	if oldClientObjExisting && oldServerObjExisting { // Both existing, do nothing
		return nil
	}

	if oldServerObjExisting {
		if err := r.client.Delete(ctx, oldSObj); err != nil {
			return fmt.Errorf("failed to delete malformed nats server auth secret, err: %w", err)
		}
		r.logger.Infow("Deleted malformed nats server auth secret successfully")
	}

	if oldClientObjExisting {
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
	if r.isbSvc.Spec.JetStream.TLS {
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
		fmt.Sprintf("%s.%s.svc.cluster.local", generateJetStreamServiceName(r.isbSvc), r.isbSvc.Namespace),
		fmt.Sprintf("%s.%s.svc", generateJetStreamServiceName(r.isbSvc), r.isbSvc.Namespace),
	}
	serverKeyPEM, serverCertPEM, caCertPEM, err := tls.CreateCerts(certOrg, hosts, time.Now().Add(10*365*24*time.Hour), true, false)
	if err != nil {
		return fmt.Errorf("failed to generate JetStream server CERT, %w", err)
	}
	// Generate cluster Cert
	clusterNodeHosts := []string{
		fmt.Sprintf("*.%s.%s.svc.cluster.local", generateJetStreamServiceName(r.isbSvc), r.isbSvc.Namespace),
		fmt.Sprintf("*.%s.%s.svc", generateJetStreamServiceName(r.isbSvc), r.isbSvc.Namespace),
	}
	clusterKeyPEM, clusterCertPEM, clusterCACertPEM, err := tls.CreateCerts(certOrg, clusterNodeHosts, time.Now().Add(10*365*24*time.Hour), true, true)
	if err != nil {
		return fmt.Errorf("failed to generate JetStream cluster CERT, %w", err)
	}

	serverObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbSvc.Namespace,
			Name:      generateJetStreamServerSecretName(r.isbSvc),
			Labels:    r.labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbSvc.GetObjectMeta(), dfv1.ISBGroupVersionKind),
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
			Namespace: r.isbSvc.Namespace,
			Name:      generateJetStreamClientAuthSecretName(r.isbSvc),
			Labels:    r.labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbSvc.GetObjectMeta(), dfv1.ISBGroupVersionKind),
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
	svcName := generateJetStreamServiceName(r.isbSvc)
	ssName := generateJetStreamStatefulSetName(r.isbSvc)
	replicas := r.isbSvc.Spec.JetStream.GetReplicas()
	var routes []string
	for j := 0; j < replicas; j++ {
		routes = append(routes, fmt.Sprintf("nats://%s-%s.%s.%s.svc:%s", ssName, strconv.Itoa(j), svcName, r.isbSvc.Namespace, strconv.Itoa(int(clusterPort))))
	}
	encryptionSettings := ""
	if r.isbSvc.Spec.JetStream.Encryption {
		encryptionSettings = "key: $JS_KEY"
	}
	clusterTLSConfig := ""
	if r.isbSvc.Spec.JetStream.TLS {
		clusterTLSConfig = `
  tls {
    cert_file: "/etc/nats-config/cluster-server-cert.pem"
	key_file: "/etc/nats-config/cluster-server-key.pem"
	ca_file: "/etc/nats-config/cluster-ca-cert.pem"
  }
`
	}
	// Merge Nats settings
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(r.config.GetISBSvcConfig().JetStream.Settings)); err != nil {
		return fmt.Errorf("invalid jetstream settings in global configuration, %w", err)
	}
	if x := r.isbSvc.Spec.JetStream.Settings; x != nil {
		if err := v.MergeConfig(bytes.NewBufferString(*x)); err != nil {
			return fmt.Errorf("failed to merge customized jetstream settings, %w", err)
		}
	}
	var confTpl *template.Template
	if replicas > 2 {
		confTpl = template.Must(template.ParseFS(jetStremAssets, "assets/jetstream/nats-cluster.conf"))
	} else {
		confTpl = template.Must(template.ParseFS(jetStremAssets, "assets/jetstream/nats.conf"))
	}

	var confTplOutput bytes.Buffer
	if err := confTpl.Execute(&confTplOutput, struct {
		ClusterName        string
		MonitorPort        string
		ClusterPort        string
		ClientPort         string
		Routes             string
		MaxPayload         string
		MaxMemoryStore     string
		MaxFileStore       string
		EncryptionSettings string
		TLSConfig          string
	}{
		ClusterName:        r.isbSvc.Name,
		MonitorPort:        strconv.Itoa(int(monitorPort)),
		ClusterPort:        strconv.Itoa(int(clusterPort)),
		ClientPort:         strconv.Itoa(int(clientPort)),
		Routes:             strings.Join(routes, ","),
		MaxPayload:         v.GetString("max_payload"),
		MaxFileStore:       v.GetString("max_file_store"),
		MaxMemoryStore:     v.GetString("max_memory_store"),
		EncryptionSettings: encryptionSettings,
		TLSConfig:          clusterTLSConfig,
	}); err != nil {
		return fmt.Errorf("failed to parse nats config template, error: %w", err)
	}
	data[dfv1.JetStreamConfigMapKey] = confTplOutput.String()

	hash := sharedutil.MustHash(data)
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbSvc.Namespace,
			Name:      generateJetStreamConfigMapName(r.isbSvc),
			Labels:    r.labels,
			Annotations: map[string]string{
				dfv1.KeyHash: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbSvc.GetObjectMeta(), dfv1.ISBGroupVersionKind),
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
			r.recorder.Event(r.isbSvc, corev1.EventTypeNormal, "JetStreamConfigMap", "Created jetstream configmap successfully")
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
		r.recorder.Event(r.isbSvc, corev1.EventTypeNormal, "JetStreamConfigMap", "Updated jetstream configmap successfully")
	}
	return nil
}

func (r *jetStreamInstaller) Uninstall(ctx context.Context) error {
	// Clean up metrics
	_ = reconciler.JetStreamISBSvcReplicas.DeleteLabelValues(r.isbSvc.Namespace, r.isbSvc.Name)
	// TODO: (k8s 1.27) Remove this once we deprecate the support for k8s < 1.27
	if !dfv1.IsPVCRetentionPolicySupported() {
		return r.uninstallPVCs(ctx)
	}
	return nil
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
		Namespace:     r.isbSvc.Namespace,
		LabelSelector: labels.SelectorFromSet(r.labels),
	})
	if err != nil {
		return nil, err
	}
	return pvcl.Items, nil
}

func (r *jetStreamInstaller) CheckChildrenResourceStatus(ctx context.Context) error {
	var isbStatefulSet appv1.StatefulSet
	if err := r.client.Get(ctx, client.ObjectKey{
		Namespace: r.isbSvc.Namespace,
		Name:      generateJetStreamStatefulSetName(r.isbSvc),
	}, &isbStatefulSet); err != nil {
		if apierrors.IsNotFound(err) {
			r.isbSvc.Status.MarkChildrenResourceUnHealthy("GetStatefulSetFailed",
				"StatefulSet not found, might be still under creation")
			return nil
		}
		r.isbSvc.Status.MarkChildrenResourceUnHealthy("GetStatefulSetFailed", err.Error())
		return err
	}
	// calculate the status of the InterStepBufferService by statefulset status and update the status of isbSvc
	if status, reason, msg := reconciler.CheckStatefulSetStatus(&isbStatefulSet); status {
		r.isbSvc.Status.MarkChildrenResourceHealthy(reason, msg)
	} else {
		r.isbSvc.Status.MarkChildrenResourceUnHealthy(reason, msg)
	}
	return nil
}

func generateJetStreamServerSecretName(isbSvc *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-server", isbSvc.Name)
}

func generateJetStreamClientAuthSecretName(isbSvc *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-client-auth", isbSvc.Name)
}

func generateJetStreamServiceName(isbSvc *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-svc", isbSvc.Name)
}

func generateJetStreamStatefulSetName(isbSvc *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js", isbSvc.Name)
}

func generateJetStreamConfigMapName(isbSvc *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-config", isbSvc.Name)
}

func generateJetStreamPVCName(isbSvc *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-js-vol", isbSvc.Name)
}
