package watchdog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/numaproj/numaflow/controllers"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

// watchdogReconciler is used to watch isb service pod changes.
type watchdogReconciler struct {
	client client.Client
	scheme *runtime.Scheme

	config *controllers.GlobalConfig
	logger *zap.SugaredLogger
}

func NewReconciler(client client.Client, scheme *runtime.Scheme, config *controllers.GlobalConfig, logger *zap.SugaredLogger) reconcile.Reconciler {
	return &watchdogReconciler{client: client, scheme: scheme, config: config, logger: logger}
}

func (r *watchdogReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sts := &appv1.StatefulSet{}
	if err := r.client.Get(ctx, req.NamespacedName, sts); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get statefulset", zap.Any("request", req), zap.Error(err))
		return ctrl.Result{}, err
	}
	isbSvcName, ok := sts.GetLabels()[dfv1.KeyISBSvcName]
	if !ok {
		// Not ISB Service StatefulSet
		return reconcile.Result{}, nil
	}
	isbSvc := &dfv1.InterStepBufferService{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: isbSvcName}, isbSvc); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.logger.Errorw("Unable to get InterStepBufferService", zap.String("isbSvc", isbSvcName), zap.Error(err))
		return ctrl.Result{}, err
	}
	if !isbSvc.DeletionTimestamp.IsZero() {
		// Ignore ISB Svc deletion
		return reconcile.Result{}, nil
	}
	log := r.logger.With("namespace", isbSvc.Namespace).With("isbSvc", isbSvc.Name)
	ctx = logging.WithLogger(ctx, log)
	if sts.GetCreationTimestamp().After(time.Now().Add(-5 * time.Minute)) {
		// Ignore new created StatefulSet Pod changes,
		// Use 5 minutes as a judgement rule
		log.Info("Ignore new created StatefulSet pod changes")
		return reconcile.Result{}, nil
	}

	// Find impacted pipeline names
	pipelineNames := []string{}
	vertexList := &dfv1.VertexList{}
	if err := r.client.List(ctx, vertexList, &client.ListOptions{Namespace: isbSvc.Namespace}); err != nil {
		log.Errorw("Failed to find vertex list, %w", err)
		return ctrl.Result{}, err
	}
	for _, v := range vertexList.Items {
		vertexISBName := dfv1.DefaultISBSvcName
		if v.Spec.InterStepBufferServiceName != "" {
			vertexISBName = v.Spec.InterStepBufferServiceName
		}
		if isbSvcName != vertexISBName {
			continue
		}
		if !sharedutil.StringSliceContains(pipelineNames, v.Spec.PipelineName) {
			pipelineNames = append(pipelineNames, v.Spec.PipelineName)
		}
	}
	if len(pipelineNames) == 0 {
		log.Infow("Not a pipeline impacted by ISB service pods change", zap.String("isbSvc", isbSvcName))
		return ctrl.Result{}, nil
	}
	// Find all the source vertices of impacted pipelines
	// Do not look for all the vertices with ISB service name because a pipeline may use different ISB service for its vertices in the future
	selector, _ := labels.Parse(dfv1.KeyPipelineName + " in (" + strings.Join(pipelineNames, ",") + ")")
	vList := &dfv1.VertexList{}
	if err := r.client.List(ctx, vList, &client.ListOptions{Namespace: isbSvc.Namespace, LabelSelector: selector}); err != nil {
		log.Errorw("Failed to find vertex list with pipeline names, %w", err)
		return ctrl.Result{}, err
	}
	for _, v := range vList.Items {
		if !v.IsASource() { // Only scale source vertices
			continue
		}
		// TODO: Scale down source Vertex to 0 or 1.
	}

	lostHosts, err := r.getLostHosts(ctx, req.Namespace, isbSvcName, sts.Name, int(*sts.Spec.Replicas))
	if err != nil {
		log.Errorw("Failed to get lost pods, %w", err)
		return ctrl.Result{}, err
	}
	if len(lostHosts) > 2 {
		log.Warnf("Lost too many hosts: %d", len(lostHosts))
	}

	if isbSvc.Spec.Redis != nil { // Do nothing with Redis
		return ctrl.Result{}, nil
	}

	jsConfig := isbSvc.Status.Config.JetStream
	if jsConfig == nil {
		log.Info("Not JetStream ISB Svc")
		return ctrl.Result{}, nil
	}
	if isbSvc.Spec.JetStream.GetReplicas() <= 3 {
		log.Info("Can not massage a JetStream InterStepBufferService with replicas <= 3")
		return ctrl.Result{}, nil
	}

	userSecret := &corev1.Secret{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: jsConfig.Auth.User.Name}, userSecret); err != nil {
		return ctrl.Result{}, err
	}
	user := userSecret.StringData[jsConfig.Auth.User.Key]
	passwordSecret := &corev1.Secret{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: jsConfig.Auth.Password.Name}, passwordSecret); err != nil {
		return ctrl.Result{}, err
	}
	password := passwordSecret.StringData[jsConfig.Auth.Password.Key]
	go func(natsUrl, user, password string, lostHosts []string) {
		if err := wait.ExponentialBackoffWithContext(ctx, sharedutil.DefaultRetryBackoff, func() (bool, error) {
			err := massageJetStreamIsbSvc(ctx, natsUrl, user, password, lostHosts)
			if err != nil {
				log.Errorw("Failed to massage JetStream ISB Svc", zap.Error(err))
				return false, err
			}
			return true, nil
		}); err != nil {
			log.Errorw("Failed to massage JetStream ISB Svc after retrying.", zap.Error(err))
		}
	}(jsConfig.URL, user, password, lostHosts)
	return ctrl.Result{}, nil
}

func (r *watchdogReconciler) getLostHosts(ctx context.Context, namespace, isbSvcName, stsName string, replicas int) ([]string, error) {
	podList := &corev1.PodList{}
	selector, _ := labels.Parse(dfv1.KeyISBSvcName + "=" + isbSvcName + "," + dfv1.KeyManagedBy + "=" + dfv1.ControllerISBSvc)
	if err := r.client.List(ctx, podList, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
		return nil, err
	}
	lost, good := []string{}, []string{}
	for _, p := range podList.Items {
		if !p.DeletionTimestamp.IsZero() { // Deleting
			lost = append(lost, p.Name)
			continue
		}
		if p.Status.Phase != corev1.PodRunning {
			lost = append(lost, p.Name)
			continue
		}
		allReady := true
		for _, s := range p.Status.ContainerStatuses {
			if !s.Ready {
				allReady = false
				break
			}
		}
		if !allReady {
			lost = append(lost, p.Name)
			continue
		}
		good = append(good, p.Name)
	}
	for i := 0; i < replicas; i++ {
		p := fmt.Sprintf("%s-%v", stsName, i)
		// Not found in the pod list, which means it's been deleted before creating a new pod
		if !sharedutil.StringSliceContains(lost, p) && !sharedutil.StringSliceContains(good, p) {
			lost = append(lost, p)
		}
	}
	return lost, nil
}

// massageJetStreamIsbSvc is used to do some management on the JetStream cluster.
// Doing it directly in the controller instead of creating a k8s job for high efficiency
func massageJetStreamIsbSvc(ctx context.Context, natsUrl, user, password string, lostHosts []string) error {
	log := logging.FromContext(ctx)
	opts := []nats.Option{
		nats.UserInfo(user, password),
	}

	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to nats url=%s: %w", natsUrl, err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	mgr, err := jsm.New(nc, jsm.WithTimeout(2*time.Second))
	if err != nil {
		return fmt.Errorf("failed to get a jsm manager, %w", err)
	}
	for s := range js.StreamsInfo() {
		stream, err := mgr.LoadStream(s.Config.Name)
		if err != nil {
			return fmt.Errorf("failed to load stream %q, %w", s.Config.Name, err)
		}
		// Only peer-remove replicas. When leader is down.
		// In case it's picked as a replica, the pod creation triggered reconciliation will peer-remove it again.
		for _, r := range s.Cluster.Replicas {
			if sharedutil.StringSliceContains(lostHosts, r.Name) {
				if err := stream.RemoveRAFTPeer(r.Name); err != nil {
					return fmt.Errorf("failed to peer-remove %s, %w", r.Name, err)
				}
				log.Infof("Sent peer-remove request to remove %q from stream %q", r.Name, s.Config.Name)
			}
		}
	}
	return nil
}
