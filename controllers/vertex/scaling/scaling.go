package scaling

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type Scaler struct {
	client    client.Client
	vertexMap map[string]*list.Element
	// List of the vertex namespaced name, format is "namespace/name"
	vertexList *list.List
	lock       sync.RWMutex
	options    *options
}

func NewScaler(client client.Client, opts ...Option) *Scaler {
	s := &Scaler{
		client:     client,
		options:    defaultOptions(),
		vertexMap:  make(map[string]*list.Element),
		vertexList: list.New(),
		lock:       sync.RWMutex{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s.options)
		}
	}
	return s
}

// Contains returns if the Scaler contains the key.
func (s *Scaler) Contains(key string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.vertexMap[key]
	return ok
}

// Length returns how many vetices are being watched for auto scaling
func (s *Scaler) Length() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.vertexList.Len()
}

// StartWatching put a key (namespace/name) into the Scaler
func (s *Scaler) StartWatching(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.vertexMap[key]; !ok {
		s.vertexMap[key] = s.vertexList.PushBack(key)
	}
}

// StopWatching stops auto scaling on the key (namespace/name)
func (s *Scaler) StopWatching(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if e, ok := s.vertexMap[key]; ok {
		_ = s.vertexList.Remove(e)
		delete(s.vertexMap, key)
	}
}

func (s *Scaler) scale(ctx context.Context, id int, keyCh <-chan string) {
	log := logging.FromContext(ctx)
	log.Infof("Started sacling worker %v", id)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopped scaling worker %v", id)
			return
		case key := <-keyCh:
			if err := s.scaleOneVertex(ctx, key); err != nil {
				log.Errorw("Failed to scale a vertex", zap.String("vertex", key), zap.Error(err))
			}
		}
	}
}

// The detail logic of scaling a vertex
func (s *Scaler) scaleOneVertex(ctx context.Context, key string) error {
	log := logging.FromContext(ctx)
	strs := strings.Split(key, "/")
	if len(strs) != 2 {
		return fmt.Errorf("invalid key %q", key)
	}
	namespace := strs[0]
	vertexFullName := strs[1]
	vertex := &dfv1.Vertex{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: vertexFullName}, vertex); err != nil {
		if apierrors.IsNotFound(err) {
			s.StopWatching(key)
			log.Infow("No corresponding Vertex found, stopped watching", zap.String("vertex", key))
			return nil
		}
		return fmt.Errorf("failed to query vertex object of key %q, %w", key, err)
	}
	if !vertex.GetDeletionTimestamp().IsZero() {
		s.StopWatching(key)
		log.Debugw("Vertex being deleted", zap.String("vertex", key))
		return nil
	}
	if vertex.Spec.Scale.Disabled { // Auto scaling disabled
		s.StopWatching(key) // Remove it in case it's watched
		return nil
	}
	if time.Since(vertex.Status.LastScaledAt.Time).Seconds() < float64(vertex.Spec.Scale.GetCooldownSeconds()) {
		log.Debugw("Cooldown period, skip scaling.")
		return nil
	}
	pl := &dfv1.Pipeline{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: vertex.Spec.PipelineName}, pl); err != nil {
		if apierrors.IsNotFound(err) {
			s.StopWatching(key)
			log.Infow("No corresponding Pipeline found, stopped watching", zap.String("vertex", key))
			return nil
		}
		return fmt.Errorf("failed to query Pipeline object of key %q, %w", key, err)
	}
	if !pl.GetDeletionTimestamp().IsZero() {
		s.StopWatching(key)
		log.Debugw("Corresponding Pipeline being deleted", zap.String("vertex", key))
		return nil
	}
	if pl.Spec.Lifecycle.DesiredPhase != dfv1.PipelinePhaseRunning {
		log.Debugw("Corresponding Pipeline not in Running state", zap.String("vertex", key))
		return nil
	}
	if int(vertex.Status.Replicas) != vertex.Spec.GetReplicas() {
		log.Debugf("Vertex %s has is under processing, replicas mismatch", vertex.Name)
		return nil
	}
	if vertex.Status.Replicas == 0 { // Was scaled to 0
		if seconds := time.Since(vertex.Status.LastScaledAt.Time).Seconds(); seconds >= float64(vertex.Spec.Scale.GetZeroReplicaSleepSeconds()) {
			log.Debugf("Vertex %s has slept %v seconds, scaling up to peek", vertex.Name, seconds)
			return s.patchVertexReplicas(ctx, vertex, 1)
		}
		return nil
	}
	// TODO: cache
	dClient, err := daemonclient.NewDaemonServiceClient(pl.GetDaemonServiceURL())
	if err != nil {
		return fmt.Errorf("failed to get daemon service client for pipeline %s, %w", pl.Name, err)
	}
	vMetrics, err := dClient.GetVertexMetrics(ctx, namespace, pl.Name, vertex.Spec.Name)
	if err != nil {
		return fmt.Errorf("failed to get metrics of vertex key %q, %w", key, err)
	}
	// Avg rate and pending for auto-scaling are both in the map with key "default", see "pkg/metrics/metrics.go".
	rate, existing := vMetrics.ProcessingRates["default"]
	if !existing || rate < 0 || rate == isb.RateNotAvailable { // Rate not available
		log.Debugf("Vertex %s has no rate information, skip scaling", vertex.Name)
		return nil
	}
	pending, existing := vMetrics.Pendings["default"]
	if !existing || pending < 0 || pending == isb.PendingNotAvailable {
		// Pending not available, we don't do anything
		log.Debugf("Vertex %s has no pending messages information, skip scaling", vertex.Name)
		return nil
	}
	availBufferLength := int64(0)
	if !vertex.IsASource() { // Only non-source vertex has buffer to read
		bufferName := vertex.GetFromBuffers()[0].Name
		if bInfo, err := dClient.GetPipelineBuffer(ctx, pl.Name, bufferName); err != nil {
			return fmt.Errorf("failed to get the read buffer information of vertex %q, %w", vertex.Name, err)
		} else {
			if bInfo.BufferLength == nil || bInfo.BufferUsageLimit == nil {
				return fmt.Errorf("invalid read buffer information of vertex %q, length or usage limit is missing", vertex.Name)
			}
			availBufferLength = int64(float64(*bInfo.BufferLength) * *bInfo.BufferUsageLimit)
		}
	}

	if desired, err := s.desiredReplicas(ctx, vertex, rate, pending, availBufferLength); err != nil {
		return fmt.Errorf("failed to calculate desired replicas for vertex %q, %w", vertex.Name, err)
	} else {
		current := int32(vertex.Spec.GetReplicas())
		max := vertex.Spec.Scale.GetMaxReplicas()
		min := vertex.Spec.Scale.GetMinReplicas()
		if desired > max {
			desired = max
		}
		if desired < min {
			desired = min
		}
		if current > max || current < min { // Someone might have manually scaled up/down the vertex
			return s.patchVertexReplicas(ctx, vertex, desired)
		}
		if desired < current {
			return s.patchVertexReplicas(ctx, vertex, current-1) // We scale down gradually
		}
		if desired > current {
			return s.patchVertexReplicas(ctx, vertex, current+1) // We scale up gradually
		}
	}
	return nil
}

func (s *Scaler) desiredReplicas(ctx context.Context, vertex *dfv1.Vertex, rate float64, pending int64, availBufferLength int64) (int32, error) {
	if rate == 0 && pending == 0 { // This could scale down to 0
		return 0, nil
	}
	if vertex.IsASource() {
		singleRate := rate / float64(vertex.Status.Replicas)
		desired := int32(pending / (int64(singleRate) * int64(vertex.Spec.Scale.GetTargetProcessingSeconds()))) // Expect to finish all the pending in N seconds
		if desired == 0 {
			desired = 1
		}
		return desired, nil
	} else {
		singleContribution := (availBufferLength - pending) / int64(vertex.Status.Replicas)
		if singleContribution < 0 {
			// Simply return a doubled replica number if the pending messages are more than available buffer length
			return 2 * int32(vertex.Status.Replicas), nil
		}
		desired := int32((availBufferLength * int64(vertex.Spec.Scale.GetTargetBufferUsage()) / 100) / singleContribution)
		// TODO: Consider back pressure for UDF
		if desired == 0 {
			desired = 1
		}
		return desired, nil
	}
}

// Start function starts the auto-scaling worker group
func (s *Scaler) Start(ctx context.Context) {
	log := logging.FromContext(ctx)
	keyCh := make(chan string)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 1; i <= s.options.workers; i++ {
		go s.scale(ctx, i, keyCh)
	}

	assign := func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.vertexList.Len() == 0 {
			return
		}
		e := s.vertexList.Front()
		if key, ok := e.Value.(string); ok {
			s.vertexList.MoveToBack(e)
			keyCh <- key
		}
	}

	n := 30000 // Milliseconds of the interval that each vertex key gets assigned.
	for {
		select {
		case <-ctx.Done():
			log.Info("Exiting scaling controller assigning job")
			return
		default:
			assign()
		}
		// Make sure each of the key will be assigned at most every N milliseconds.
		time.Sleep(time.Millisecond * time.Duration(func() int {
			l := s.Length()
			if l == 0 {
				return n
			}
			result := n / l
			if result > 0 {
				return result
			}
			return 1
		}()))
	}
}

func (s *Scaler) patchVertexReplicas(ctx context.Context, vertex *dfv1.Vertex, desiredReplicas int32) error {
	log := logging.FromContext(ctx)
	origin := vertex.Spec.Replicas
	vertex.Spec.Replicas = pointer.Int32(desiredReplicas)
	body, err := json.Marshal(vertex)
	if err != nil {
		return fmt.Errorf("failed to marshal vertex object to json, %w", err)
	}
	if err := s.client.Patch(ctx, vertex, client.RawPatch(types.MergePatchType, body)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to patch vertex replicas, %w", err)
	}
	log.Infow("Auto scaling - vertex replicas changed", zap.Int32p("from", origin), zap.Int32("to", desiredReplicas), zap.String("pipeline", vertex.Spec.PipelineName), zap.String("vertex", vertex.Spec.Name))
	return nil
}
