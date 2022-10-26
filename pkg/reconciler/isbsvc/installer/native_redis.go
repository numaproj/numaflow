package installer

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"text/template"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	redisPort        = int32(6379)
	redisMetricsPort = int32(9121)
	sentinelPort     = int32(26379)
	defaultUser      = "default"
)

var (
	//go:embed assets/redis/scripts/*
	redisScriptsAssets embed.FS
	//go:embed assets/redis/health/*
	redisHealthAssets embed.FS
	//go:embed assets/redis/config/*
	redisConfigAssets embed.FS
)

type redisInstaller struct {
	client client.Client
	isbs   *dfv1.InterStepBufferService
	config *reconciler.GlobalConfig
	labels map[string]string
	logger *zap.SugaredLogger
}

func NewNativeRedisInstaller(client client.Client, isbs *dfv1.InterStepBufferService, config *reconciler.GlobalConfig, labels map[string]string, logger *zap.SugaredLogger) Installer {
	return &redisInstaller{
		client: client,
		isbs:   isbs,
		config: config,
		labels: labels,
		logger: logger.With("isbs", isbs.Name),
	}
}

func (r *redisInstaller) Install(ctx context.Context) (*dfv1.BufferServiceConfig, error) {
	if redis := r.isbs.Spec.Redis; redis == nil {
		return nil, fmt.Errorf("invalid native redis isbs spec")
	}
	r.isbs.Status.SetType(dfv1.ISBSvcTypeRedis)
	if err := r.createScriptsConfigMap(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis scripts ConfigMap", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisScriptsConfigMapFailed", err.Error())
		return nil, err
	}
	if err := r.createHealthConfigMap(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis health ConfigMap", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisHealthConfigMapFailed", err.Error())
		return nil, err
	}
	if err := r.createConfConfigMap(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis config ConfigMap", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisConfConfigMapFailed", err.Error())
		return nil, err
	}
	if err := r.createAuthCredentialSecret(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis password", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisPasswordSecretFailed", err.Error())
		return nil, err
	}
	if err := r.createRedisService(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis Service", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisServiceFailed", err.Error())
		return nil, err
	}
	if err := r.createRedisHeadlessService(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis Headless Service", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisHeadlessServiceFailed", err.Error())
		return nil, err
	}
	if err := r.createStatefulSet(ctx); err != nil {
		r.logger.Errorw("Failed to create Redis StatefulSet", zap.Error(err))
		r.isbs.Status.MarkDeployFailed("RedisStatefulSetFailed", err.Error())
		return nil, err
	}

	r.isbs.Status.MarkDeployed()
	return &dfv1.BufferServiceConfig{
		Redis: &dfv1.RedisConfig{
			SentinelURL: fmt.Sprintf("%s.%s.svc:%v", generateRedisServiceName(r.isbs), r.isbs.Namespace, sentinelPort),
			MasterName:  dfv1.DefaultRedisSentinelMasterName,
			User:        defaultUser,
			Password: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: generateRedisCredentialSecretName(r.isbs),
				},
				Key: dfv1.RedisAuthSecretKey,
			},
			SentinelPassword: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: generateRedisCredentialSecretName(r.isbs),
				},
				Key: dfv1.RedisAuthSecretKey,
			},
		},
	}, nil
}

func (r *redisInstaller) createAuthCredentialSecret(ctx context.Context) error {
	password := sharedutil.RandomString(16)
	obj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateRedisCredentialSecretName(r.isbs),
			Labels:    r.labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(r.isbs.GetObjectMeta(), dfv1.ISBGroupVersionKind),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			dfv1.RedisAuthSecretKey: []byte(password),
		},
	}
	old := &corev1.Secret{}
	if err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), old); err != nil {
		if apierrors.IsNotFound(err) {
			if err := r.client.Create(ctx, obj); err != nil {
				return fmt.Errorf("failed to create redis auth credential secret, err: %w", err)
			}
			r.logger.Infow("Created redis auth credential secret successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis auth credential secret is existing, err: %w", err)
		}
	}
	// If it's existing, do not make any change.
	return nil
}

func (r *redisInstaller) createScriptsConfigMap(ctx context.Context) error {
	data := make(map[string]string)

	params := struct {
		ServiceName         string
		HeadlessServiceName string
		StatefulSetName     string
		Namespace           string
		Replicas            int
		RedisPort           int32
		SentinelPort        int32
	}{
		ServiceName:         generateRedisServiceName(r.isbs),
		HeadlessServiceName: generateRedisHeadlessServiceName(r.isbs),
		StatefulSetName:     generateRedisStatefulSetName(r.isbs),
		Namespace:           r.isbs.Namespace,
		Replicas:            r.isbs.Spec.Redis.Native.GetReplicas(),
		RedisPort:           redisPort,
		SentinelPort:        sentinelPort,
	}

	tplFileNames := []string{"prestop-sentinel.sh", "start-node.sh", "start-sentinel.sh"}
	for _, fileName := range tplFileNames {
		t := template.Must(template.ParseFS(redisScriptsAssets, fmt.Sprintf("assets/redis/scripts/%s", fileName)))
		var tplOutput bytes.Buffer
		if err := t.Execute(&tplOutput, params); err != nil {
			return fmt.Errorf("failed to parse script templates, error: %w", err)
		}
		data[fileName] = tplOutput.String()
	}

	fileNames := []string{"prestop-redis.sh"}
	for _, fileName := range fileNames {
		if d, err := redisScriptsAssets.ReadFile(fmt.Sprintf("assets/redis/scripts/%s", fileName)); err != nil {
			return fmt.Errorf("failed to read file assets/redis/scripts/%s", fileName)
		} else {
			data[fileName] = string(d)
		}
	}

	hash := sharedutil.MustHash(data)
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateScriptsConfigMapName(r.isbs),
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
				return fmt.Errorf("failed to create redis scripts configmap, err: %w", err)
			}
			r.logger.Info("Created redis scripts configmap successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis scripts configmap is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Data = data
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update redis scripts configmap, err: %w", err)
		}
		r.logger.Info("Updated redis scripts configmap successfully")
	}
	return nil
}

func (r *redisInstaller) createHealthConfigMap(ctx context.Context) error {
	data := make(map[string]string)
	fileNames := []string{"parse_sentinels.awk", "ping_liveness_local.sh", "ping_liveness_local_and_master.sh", "ping_liveness_master.sh", "ping_readiness_local.sh", "ping_readiness_local_and_master.sh", "ping_readiness_master.sh", "ping_sentinel.sh"}
	for _, fileName := range fileNames {
		if d, err := redisHealthAssets.ReadFile(fmt.Sprintf("assets/redis/health/%s", fileName)); err != nil {
			return fmt.Errorf("failed to read file assets/redis/health/%s", fileName)
		} else {
			data[fileName] = string(d)
		}
	}

	hash := sharedutil.MustHash(data)
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateHealthConfigMapName(r.isbs),
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
				return fmt.Errorf("failed to create redis health configmap, err: %w", err)
			}
			r.logger.Info("Created redis health configmap successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis health configmap is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Data = data
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update redis health configmap, err: %w", err)
		}
		r.logger.Info("Updated redis health configmap successfully")
	}
	return nil
}

func (r *redisInstaller) createConfConfigMap(ctx context.Context) error {
	data := make(map[string]string)
	redisConf, masterConf, replicaConf, sentinelConf := "", "", "", ""
	if x := r.config.ISBSvc.Redis.Settings; x != nil {
		if x.Redis != "" {
			redisConf = x.Redis
		}
		if x.Master != "" {
			masterConf = x.Master
		}
		if x.Replica != "" {
			replicaConf = x.Replica
		}
		if x.Sentinel != "" {
			sentinelConf = x.Sentinel
		}
	}
	if x := r.isbs.Spec.Redis.Native.Settings; x != nil {
		if x.Redis != "" {
			redisConf = x.Redis
		}
		if x.Master != "" {
			masterConf = x.Master
		}
		if x.Replica != "" {
			replicaConf = x.Replica
		}
		if x.Sentinel != "" {
			sentinelConf = x.Sentinel
		}
	}

	t := template.Must(template.ParseFS(redisConfigAssets, "assets/redis/config/sentinel.conf"))
	var tplOutput bytes.Buffer
	if err := t.Execute(&tplOutput, struct {
		SentinelPort        int32
		StatefulSetName     string
		HeadlessServiceName string
		Namespace           string
		SentinelSettings    string
		RedisPort           int32
		Quorum              int
	}{
		SentinelPort:        sentinelPort,
		StatefulSetName:     generateRedisStatefulSetName(r.isbs),
		HeadlessServiceName: generateRedisHeadlessServiceName(r.isbs),
		Namespace:           r.isbs.Namespace,
		SentinelSettings:    sentinelConf,
		RedisPort:           redisPort,
		Quorum:              r.isbs.Spec.Redis.Native.GetReplicas() / 2,
	}); err != nil {
		return fmt.Errorf("failed to parse sentinel config template, error: %w", err)
	}
	data["sentinel.conf"] = tplOutput.String()

	redisTpl := template.Must(template.ParseFS(redisConfigAssets, "assets/redis/config/redis.conf"))
	var redisTplOutput bytes.Buffer
	if err := redisTpl.Execute(&redisTplOutput, struct {
		RedisSettings string
	}{
		RedisSettings: redisConf,
	}); err != nil {
		return fmt.Errorf("failed to parse redis config template, error: %w", err)
	}
	data["redis.conf"] = redisTplOutput.String()

	masterTpl := template.Must(template.ParseFS(redisConfigAssets, "assets/redis/config/master.conf"))
	var masterTplOutput bytes.Buffer
	if err := masterTpl.Execute(&masterTplOutput, struct {
		MasterSettings string
	}{
		MasterSettings: masterConf,
	}); err != nil {
		return fmt.Errorf("failed to parse redis master config template, error: %w", err)
	}
	data["master.conf"] = masterTplOutput.String()

	replicaTpl := template.Must(template.ParseFS(redisConfigAssets, "assets/redis/config/replica.conf"))
	var replicaTplOutput bytes.Buffer
	if err := replicaTpl.Execute(&replicaTplOutput, struct {
		ReplicaSettings string
	}{
		ReplicaSettings: replicaConf,
	}); err != nil {
		return fmt.Errorf("failed to parse redis replica config template, error: %w", err)
	}
	data["replica.conf"] = replicaTplOutput.String()

	hash := sharedutil.MustHash(data)
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateRedisConfigMapName(r.isbs),
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
				return fmt.Errorf("failed to create redis config configmap, err: %w", err)
			}
			r.logger.Info("Created redis config configmap successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis config configmap is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Data = data
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update redis config configmap, err: %w", err)
		}
		r.logger.Info("Updated redis config configmap successfully")
	}
	return nil
}

func (r *redisInstaller) createRedisService(ctx context.Context) error {
	spec := r.isbs.Spec.Redis.Native.GetServiceSpec(dfv1.GetRedisServiceSpecReq{
		Labels:                r.labels,
		RedisContainerPort:    redisPort,
		SentinelContainerPort: sentinelPort,
	})
	hash := sharedutil.MustHash(spec)
	obj := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateRedisServiceName(r.isbs),
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
				return fmt.Errorf("failed to create redis service, err: %w", err)
			}
			r.logger.Info("Created redis service successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis service is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Spec = spec
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update redis service, err: %w", err)
		}
		r.logger.Info("Updated redis service successfully")
	}
	return nil
}

func (r *redisInstaller) createRedisHeadlessService(ctx context.Context) error {
	spec := r.isbs.Spec.Redis.Native.GetHeadlessServiceSpec(dfv1.GetRedisServiceSpecReq{
		Labels:                r.labels,
		RedisContainerPort:    redisPort,
		SentinelContainerPort: sentinelPort,
	})
	hash := sharedutil.MustHash(spec)
	obj := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateRedisHeadlessServiceName(r.isbs),
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
				return fmt.Errorf("failed to create redis headless service, err: %w", err)
			}
			r.logger.Info("Created redis headless service successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis headless service is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Spec = spec
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update redis headless service, err: %w", err)
		}
		r.logger.Info("Updated redis headless service successfully")
	}
	return nil
}

func (r *redisInstaller) createStatefulSet(ctx context.Context) error {
	redisVersion, err := r.config.GetRedisVersion(r.isbs.Spec.Redis.Native.Version)
	if err != nil {
		return fmt.Errorf("failed to get redis version, err: %w", err)
	}
	spec := r.isbs.Spec.Redis.Native.GetStatefulSetSpec(dfv1.GetRedisStatefulSetSpecReq{
		ServiceName:               generateRedisHeadlessServiceName(r.isbs),
		Labels:                    r.labels,
		RedisImage:                redisVersion.RedisImage,
		SentinelImage:             redisVersion.SentinelImage,
		MetricsExporterImage:      redisVersion.RedisExporterImage,
		InitContainerImage:        redisVersion.InitContainerImage,
		RedisContainerPort:        redisPort,
		SentinelContainerPort:     sentinelPort,
		RedisMetricsContainerPort: redisMetricsPort,
		CredentialSecretName:      generateRedisCredentialSecretName(r.isbs),
		TLSEnabled:                false,
		PvcNameIfNeeded:           generateRedisPVCName(r.isbs),
		ConfConfigMapName:         generateRedisConfigMapName(r.isbs),
		HealthConfigMapName:       generateHealthConfigMapName(r.isbs),
		ScriptsConfigMapName:      generateScriptsConfigMapName(r.isbs),
	})
	hash := sharedutil.MustHash(spec)
	obj := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.isbs.Namespace,
			Name:      generateRedisStatefulSetName(r.isbs),
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
				return fmt.Errorf("failed to create redis statefulset, err: %w", err)
			}
			r.logger.Info("Created redis statefulset successfully")
			return nil
		} else {
			return fmt.Errorf("failed to check if redis statefulset is existing, err: %w", err)
		}
	}
	if old.GetAnnotations()[dfv1.KeyHash] != hash {
		old.Annotations[dfv1.KeyHash] = hash
		old.Spec = spec
		if err := r.client.Update(ctx, old); err != nil {
			return fmt.Errorf("failed to update redis statefulset, err: %w", err)
		}
		r.logger.Info("Updated redis statefulset successfully")
	}
	return nil
}

func (r *redisInstaller) Uninstall(ctx context.Context) error {
	return r.uninstallPVCs(ctx)
}

func (r *redisInstaller) uninstallPVCs(ctx context.Context) error {
	// StatefulSet doesn't clean up PVC, needs to do it separately
	// https://github.com/kubernetes/kubernetes/issues/55045
	pvcs, err := r.getPVCs(ctx)
	if err != nil {
		r.logger.Errorw("Failed to get PVCs created by redis statefulset when uninstalling", zap.Error(err))
		return err
	}
	for _, pvc := range pvcs {
		err = r.client.Delete(ctx, &pvc)
		if err != nil {
			r.logger.Errorw("Failed to delete pvc when uninstalling", zap.Any("pvcName", pvc.Name), zap.Error(err))
			return err
		}
		r.logger.Infow("Pvc deleted", "pvcName", pvc.Name)
	}
	return nil
}

// get PVCs created by streaming statefulset
// they have same labels as the statefulset
func (r *redisInstaller) getPVCs(ctx context.Context) ([]corev1.PersistentVolumeClaim, error) {
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

func generateRedisServiceName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-svc", isbs.Name)
}

func generateRedisHeadlessServiceName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-svc-headless", isbs.Name)
}

func generateRedisConfigMapName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-config", isbs.Name)
}

func generateScriptsConfigMapName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-scripts", isbs.Name)
}

func generateHealthConfigMapName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-health", isbs.Name)
}

func generateRedisStatefulSetName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis", isbs.Name)
}

func generateRedisCredentialSecretName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-auth", isbs.Name)
}

func generateRedisPVCName(isbs *dfv1.InterStepBufferService) string {
	return fmt.Sprintf("isbsvc-%s-redis-vol", isbs.Name)
}
