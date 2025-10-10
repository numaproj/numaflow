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

package scaling

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type Scaler struct {
	client    client.Client
	vertexMap map[string]*list.Element
	// List of the vertex namespaced name, format is "namespace/name"
	vertexList *list.List
	lock       *sync.RWMutex
	options    *options
	// Cache to store the vertex metrics such as pending message number
	vertexMetricsCache *lru.Cache[string, int64]
	daemonClientsCache *lru.Cache[string, daemonclient.DaemonClient]
}

// NewScaler returns a Scaler instance.
func NewScaler(client client.Client, opts ...Option) *Scaler {
	scalerOpts := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(scalerOpts)
		}
	}
	s := &Scaler{
		client:     client,
		options:    scalerOpts,
		vertexMap:  make(map[string]*list.Element),
		vertexList: list.New(),
		lock:       new(sync.RWMutex),
	}
	// cache daemon clients
	s.daemonClientsCache, _ = lru.NewWithEvict[string, daemonclient.DaemonClient](s.options.clientsCacheSize, func(key string, value daemonclient.DaemonClient) {
		_ = value.Close()
	})
	vertexMetricsCache, _ := lru.New[string, int64](10000)
	s.vertexMetricsCache = vertexMetricsCache
	return s
}

// Contains returns if the Scaler contains the key.
func (s *Scaler) Contains(key string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.vertexMap[key]
	return ok
}

// Length returns how many vertices are being watched for autoscaling
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

// StopWatching stops autoscaling on the key (namespace/name)
func (s *Scaler) StopWatching(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if e, ok := s.vertexMap[key]; ok {
		_ = s.vertexList.Remove(e)
		delete(s.vertexMap, key)
	}
}

// Function scale() defines each of the worker's job.
// It waits for keys in the channel, and starts a scaling job
func (s *Scaler) scale(ctx context.Context, id int, keyCh <-chan string) {
	log := logging.FromContext(ctx)
	log.Infof("Started Vertex autoscaling worker %v", id)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopped Vertex autoscaling worker %v", id)
			return
		case key := <-keyCh:
			if err := s.scaleOneVertex(ctx, key, id); err != nil {
				log.Errorw("Failed to scale a vertex", zap.String("vertex", key), zap.Error(err))
			}
		}
	}
}

// scaleOneVertex implements the detailed logic of scaling up/down a vertex.
//
// For source vertices which have both rate and pending message information,
// if there are multiple partitions, we consider the max desired replicas among all partitions.
//
//	desiredReplicas = currentReplicas * pending / (targetProcessingTime * rate)
//
// For UDF and sinks which have the read buffer information
//
//	singleReplicaContribution = (totalAvailableBufferLength - pending) / currentReplicas
//	desiredReplicas = targetAvailableBufferLength / singleReplicaContribution
//
// Back pressure factor
// When desiredReplicas > currentReplicas:
// If there's back pressure in the directly connected vertices, desiredReplicas = currentReplicas-1;
// If there's back pressure in the downstream vertices (not connected), desiredReplicas remains the same.
func (s *Scaler) scaleOneVertex(ctx context.Context, key string, worker int) error {
	log := logging.FromContext(ctx).With("worker", fmt.Sprint(worker)).With("vertexKey", key)
	log.Debugf("Working on key: %s.", key)
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
			log.Info("No corresponding Vertex found, stopped watching.")
			return nil
		}
		return fmt.Errorf("failed to query vertex object of key %q, %w", key, err)
	}
	if !vertex.GetDeletionTimestamp().IsZero() {
		s.StopWatching(key)
		log.Debug("Vertex being deleted.")
		return nil
	}
	if !vertex.Scalable() { // A vertex which is not scalable, such as a Reducer, or autoscaling disabled.
		s.StopWatching(key) // Remove it in case it's watched.
		return nil
	}
	if vertex.Status.Phase != dfv1.VertexPhaseRunning {
		log.Infof("Vertex not in Running phase, skip scaling.")
		return nil
	}
	if vertex.Spec.Lifecycle.GetDesiredPhase() != dfv1.VertexPhaseRunning {
		log.Infof("Vertex desired phase is not Running, skip scaling.")
	}
	if vertex.Status.UpdateHash != vertex.Status.CurrentHash && vertex.Status.UpdateHash != "" {
		log.Info("Vertex is updating, skip scaling.")
		return nil
	}
	secondsSinceLastScale := time.Since(vertex.Status.LastScaledAt.Time).Seconds()
	scaleDownCooldown := float64(vertex.Spec.Scale.GetScaleDownCooldownSeconds())
	scaleUpCooldown := float64(vertex.Spec.Scale.GetScaleUpCooldownSeconds())
	if secondsSinceLastScale < scaleDownCooldown && secondsSinceLastScale < scaleUpCooldown {
		// Skip scaling without needing further calculation
		log.Infof("Cooldown period, skip scaling.")
		return nil
	}
	pl := &dfv1.Pipeline{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: vertex.Spec.PipelineName}, pl); err != nil {
		if apierrors.IsNotFound(err) {
			s.StopWatching(key)
			log.Info("No corresponding Pipeline found, stopped watching.")
			return nil
		}
		return fmt.Errorf("failed to query Pipeline object of key %q, %w", key, err)
	}
	if !pl.GetDeletionTimestamp().IsZero() {
		s.StopWatching(key)
		log.Debug("Corresponding Pipeline being deleted.")
		return nil
	}
	if pl.GetDesiredPhase() != dfv1.PipelinePhaseRunning {
		log.Info("Corresponding Pipeline not in Running state, skip scaling.")
		return nil
	}
	if vertex.Status.Replicas != vertex.Status.DesiredReplicas {
		log.Infof("Vertex %s might be under processing, replicas mismatch, skip scaling.", vertex.Name)
		return nil
	}

	if vertex.Spec.Scale.GetMaxReplicas() == vertex.Spec.Scale.GetMinReplicas() {
		log.Infof("Vertex %s has same scale.min and scale.max, skip scaling.", vertex.Name)
		return nil
	}

	var err error
	daemonClient, _ := s.daemonClientsCache.Get(pl.GetDaemonServiceURL())
	if daemonClient == nil {
		daemonClient, err = daemonclient.NewGRPCDaemonServiceClient(pl.GetDaemonServiceURL())
		if err != nil {
			return fmt.Errorf("failed to get daemon service client for pipeline %s, %w", pl.Name, err)
		}
		s.daemonClientsCache.Add(pl.GetDaemonServiceURL(), daemonClient)
	}

	bufferLength, bufferLengthLimit, targetAvailableBufferLength, totalBufferLengthLimit, totalCurrentPending, err := getBufferInfos(ctx, daemonClient, pl, vertex)
	if err != nil {
		err := fmt.Errorf("error while fetching buffer info, %w", err)
		return err
	}

	if vertex.Status.Replicas == 0 { // Was scaled to 0
		// For non-source vertices,
		// Check if they have any pending messages in their owned buffers,
		// If yes, then scale them back to 1
		if !vertex.IsASource() {
			if totalCurrentPending <= 0 {
				log.Infof("Vertex %s doesn't have any pending messages, skipping scaling back to 1", vertex.Name)
				return nil
			} else {
				log.Infof("Vertex %s has some pending messages, scaling back to 1", vertex.Name)
				return s.patchVertexReplicas(ctx, vertex, 1)
			}
		}

		// For source vertices,
		// Periodically wake them up from 0 replicas to 1, to peek for the incoming messages
		if secondsSinceLastScale >= float64(vertex.Spec.Scale.GetZeroReplicaSleepSeconds()) {
			log.Infof("Vertex %s has slept %v seconds, scaling up to peek.", vertex.Name, secondsSinceLastScale)
			return s.patchVertexReplicas(ctx, vertex, 1)
		} else {
			log.Infof("Vertex %q has slept %v seconds, hasn't reached zeroReplicaSleepSeconds (%v seconds), skip scaling.", vertex.Name, secondsSinceLastScale, vertex.Spec.Scale.GetZeroReplicaSleepSeconds())
			return nil
		}
	}

	// Vertex pods are not ready yet.
	if vertex.Status.ReadyReplicas == 0 {
		log.Infof("Vertex %q  has no ready replicas, skip scaling.", vertex.Name)
		return nil
	}

	vMetrics, err := daemonClient.GetVertexMetrics(ctx, pl.Name, vertex.Spec.Name)
	if err != nil {
		return fmt.Errorf("failed to get metrics of vertex key %q, %w", key, err)
	}
	// Avg rate and pending for autoscaling are both in the map with key "default", see "pkg/metrics/metrics.go".
	// vMetrics is a map which contains metrics of all the partitions of a vertex.
	// We need to aggregate them to get the total rate and pending of the vertex.
	// If any of the partition doesn't have the rate or pending information, we skip scaling.
	// we need both aggregated and partition level metrics for scaling, because we use aggregated metrics to
	// determine whether we can scale down to 0 and for calculating back pressure, and partition level metrics to determine
	// the max desired replicas among all the partitions.
	partitionRates := make([]float64, 0)
	partitionPending := make([]int64, 0)
	totalRate := float64(0)
	totalPending := int64(0)
	for _, m := range vMetrics {
		rate, existing := m.ProcessingRates["default"]
		// If rate is not available, we skip scaling.
		if !existing || rate.GetValue() < 0 { // Rate not available
			log.Infof("Vertex %s has no rate information, skip scaling.", vertex.Name)
			return nil
		}
		partitionRates = append(partitionRates, rate.GetValue())
		totalRate += rate.GetValue()

		pending, existing := m.Pendings["default"]
		if !existing || pending.GetValue() < 0 || pending.GetValue() == dfv1.PendingNotAvailable {
			// Pending not available, we don't do anything
			log.Infof("Vertex %s has no pending messages information, skip scaling.", vertex.Name)
			return nil
		}
		totalPending += pending.GetValue()
		partitionPending = append(partitionPending, pending.GetValue())
	}

	// Add pending information to cache for back pressure calculation, if there is a backpressure it will impact all the partitions.
	// So we only need to add the total pending to the cache.
	_ = s.vertexMetricsCache.Add(key+"/pending", totalPending)
	if !vertex.IsASource() {
		_ = s.vertexMetricsCache.Add(key+"/length", totalBufferLengthLimit)
	}

	var desired int32
	current := int32(vertex.Status.Replicas)
	// if both totalRate and totalPending are 0, we scale down to 0
	// since pending contains the pending acks, we can scale down to 0.
	if totalPending == 0 && totalRate == 0 {
		desired = 0
	} else {
		desired = s.desiredReplicas(ctx, vertex, partitionRates, partitionPending, bufferLength, bufferLengthLimit, targetAvailableBufferLength)
	}

	// Check if rate limiting is configured and we're hitting the limit
	if vertex.Spec.Limits != nil && vertex.Spec.Limits.RateLimit != nil && vertex.Spec.Limits.RateLimit.Max != nil {
		var maxRateLimit float64
		// For source vertices, the rate limit is defined by how many times the `Read` is called per second multiplied
		// by the `readBatchSize`.
		if vertex.IsASource() {
			maxRateLimit = float64(*vertex.Spec.Limits.RateLimit.Max) * float64(vertex.Spec.Limits.GetReadBatchSize())
		} else {
			maxRateLimit = float64(*vertex.Spec.Limits.RateLimit.Max)
		}
		// Round up to the nearest integer because we don't want to scale up if we are almost at the rate limit
		// e.g., RateLimit is 1000, current rate is 999.9, we don't want to scale up
		currentRate := math.Ceil(totalRate)

		if currentRate >= maxRateLimit {
			// Calculate desired replicas to see if we would scale up
			current := int32(vertex.Status.Replicas)
			// Only skip if we would be scaling up
			if desired > current {
				log.Infof("Vertex %s would scale up but is at rate limit, skip scaling up.", vertex.Name)
				return nil
			}
		}
	}

	log.Infof("Calculated desired replica number of vertex %q is: %d.", vertex.Name, desired)
	maxReplicas := vertex.Spec.Scale.GetMaxReplicas()
	minReplicas := vertex.Spec.Scale.GetMinReplicas()
	if desired > maxReplicas {
		log.Infof("Calculated desired replica number %d of vertex %q is greater than max, using max %d.", desired, vertex.Name, maxReplicas)
		desired = maxReplicas
	}
	if desired < minReplicas {
		log.Infof("Calculated desired replica number %d of vertex %q is smaller than min, using min %d.", desired, vertex.Name, minReplicas)
		desired = minReplicas
	}
	if current > maxReplicas || current < minReplicas { // Someone might have manually scaled up/down the vertex
		return s.patchVertexReplicas(ctx, vertex, desired)
	}
	if desired < current {
		maxAllowedDown := int32(vertex.Spec.Scale.GetReplicasPerScaleDown())
		diff := current - desired
		if diff > maxAllowedDown {
			diff = maxAllowedDown
		}
		if secondsSinceLastScale < scaleDownCooldown {
			log.Infof("Cooldown period for scaling down, skip scaling.")
			return nil
		}
		return s.patchVertexReplicas(ctx, vertex, current-diff) // We scale down gradually
	}
	if desired > current {
		// When scaling up, need to check back pressure
		directPressure, downstreamPressure := s.hasBackPressure(*pl, *vertex)
		if directPressure {
			if current > minReplicas && current > 1 { // Scale down but not to 0
				log.Infof("Vertex %q has direct back pressure from connected vertices, decreasing one replica.", key)
				return s.patchVertexReplicas(ctx, vertex, current-1)
			} else {
				log.Infof("Vertex %q has direct back pressure from connected vertices, skip scaling.", key)
				return nil
			}
		} else if downstreamPressure {
			log.Infof("Vertex %q has back pressure in downstream vertices, skip scaling.", key)
			return nil
		}
		maxAllowedUp := int32(vertex.Spec.Scale.GetReplicasPerScaleUp())
		diff := desired - current
		if diff > maxAllowedUp {
			diff = maxAllowedUp
		}
		if secondsSinceLastScale < scaleUpCooldown {
			log.Infof("Cooldown period for scaling up, skip scaling.")
			return nil
		}
		return s.patchVertexReplicas(ctx, vertex, current+diff) // We scale up gradually
	}
	return nil
}

func (s *Scaler) desiredReplicas(_ context.Context, vertex *dfv1.Vertex, partitionProcessingRate []float64, partitionPending []int64, bufferLength, bufferLengthLimit, targetAvailableBufferLength int64) int32 {
	maxDesired := int32(1)
	// We calculate the max desired replicas based on the pending messages and processing rate for each partition.
	for i := 0; i < len(partitionPending); i++ {
		rate := partitionProcessingRate[i]
		pending := partitionPending[i]
		var desired int32
		if pending == 0 || rate == 0 {
			// Pending is 0 and rate is not 0, or rate is 0 and pending is not 0, we don't do anything.
			// Technically this would not happen because the pending includes ackpending, which means rate and pending are either both 0, or both > 0.
			// But we still keep this check here for safety.
			// in this case, we don't update the desired replicas because we don't know how many replicas are needed.
			// we cannot go with current replicas because ideally we should scale down when pending is 0 or rate is 0.
			continue
		}
		// For Soruce and non-source vertices which do not have packed buffers, use the same scaling mechanism
		if vertex.IsASource() || pending <= bufferLength-targetAvailableBufferLength {
			// For sources, we calculate the time of finishing processing the pending messages,
			// and then we know how many replicas are needed to get them done in target seconds.
			// Similarly, we do the same logic for udf and sinks when the pending is not greater than the target buffer availability expectation.
			desired = int32(math.Round(((float64(pending) / rate) / float64(vertex.Spec.Scale.GetTargetProcessingSeconds())) * float64(vertex.Status.ReadyReplicas)))
		} else {
			// For UDF and sinks, we calculate the available buffer length, and consider it is the contribution of current replicas,
			// then we figure out how many replicas are needed to keep the available buffer length at target level.
			if pending >= bufferLengthLimit {
				// Simply return current replica number + max allowed if the pending messages are more than available buffer length
				desired = int32(vertex.Status.Replicas) + int32(vertex.Spec.Scale.GetReplicasPerScaleUp())
			} else {
				singleReplicaContribution := float64(bufferLengthLimit-pending) / float64(vertex.Status.ReadyReplicas)
				desired = int32(math.Round(float64(targetAvailableBufferLength) / singleReplicaContribution))
			}
		}
		// we only scale down to zero when the total pending and total rate are both zero.
		if desired == 0 {
			desired = 1
		}
		if desired > int32(pending) && pending > 0 { // For some corner cases, we don't want to scale up to more than pending.
			desired = int32(pending)
		}
		// maxDesired is the max of all partitions
		if desired > maxDesired {
			maxDesired = desired
		}
	}
	return maxDesired
}

// Start function starts the autoscaling worker group.
// Each worker keeps picking up scaling tasks (which contains vertex keys) to calculate the desired replicas,
// and patch the vertex spec with the new replica number if needed.
func (s *Scaler) Start(ctx context.Context) error {
	log := logging.FromContext(ctx).Named("vertex-autoscaler")
	log.Info("Starting vertex autoscaler...")
	keyCh := make(chan string)
	ctx, cancel := context.WithCancel(logging.WithLogger(ctx, log))
	defer cancel()
	// Worker group
	for i := 1; i <= s.options.workers; i++ {
		go s.scale(ctx, i, keyCh)
	}

	// Function assign() moves an element in the list from the front to the back,
	// and send to the channel so that it can be picked up by a worker.
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

	// Following for loop keeps calling assign() function to assign scaling tasks to the workers.
	// It makes sure each element in the list will be assigned every N milliseconds.
	for {
		select {
		case <-ctx.Done():
			log.Info("Shutting down scaling job assigner.")
			// clear the daemon clients cache
			s.daemonClientsCache.Purge()
			return nil
		default:
			assign()
		}
		// Make sure each of the key will be assigned at most every N milliseconds.
		time.Sleep(time.Millisecond * time.Duration(func() int {
			l := s.Length()
			if l == 0 {
				return s.options.taskInterval
			}
			result := s.options.taskInterval / l
			if result > 0 {
				return result
			}
			return 1
		}()))
	}
}

// hasBackPressure checks if there's back pressure in the downstream buffers
// It returns if 2 bool values, which represent:
// 1. If there's back pressure in the connected vertices;
// 2. If there's back pressure in any of the downstream vertices.
func (s *Scaler) hasBackPressure(pl dfv1.Pipeline, vertex dfv1.Vertex) (bool, bool) {
	downstreamEdges := pl.GetDownstreamEdges(vertex.Spec.Name)
	directPressure, downstreamPressure := false, false
loop:
	for _, e := range downstreamEdges {
		if e.BufferFullWritingStrategy() == dfv1.DiscardLatest {
			// If the edge is configured to discard latest on full, we don't consider it as back pressure.
			continue
		}
		vertexKey := pl.Namespace + "/" + pl.Name + "-" + e.To
		pending, ok := s.vertexMetricsCache.Get(vertexKey + "/pending")
		if !ok { // Vertex key has not been cached, skip it.
			continue
		}
		bufferLength, ok := s.vertexMetricsCache.Get(vertexKey + "/length")
		if !ok { // Buffer length has not been cached, skip it.
			continue
		}
		if float64(pending)/float64(bufferLength) >= s.options.backPressureThreshold {
			downstreamPressure = true
			if e.From == vertex.Spec.Name {
				directPressure = true
				break loop
			}
		}
	}
	return directPressure, downstreamPressure
}

func (s *Scaler) patchVertexReplicas(ctx context.Context, vertex *dfv1.Vertex, desiredReplicas int32) error {
	log := logging.FromContext(ctx)
	origin := vertex.Spec.Replicas
	patchJson := fmt.Sprintf(`{"spec":{"replicas":%d}}`, desiredReplicas)
	if err := s.client.Patch(ctx, vertex, client.RawPatch(types.MergePatchType, []byte(patchJson))); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to patch vertex replicas, %w", err)
	}
	log.Infow("Auto scaling - vertex replicas changed.", zap.Int32p("from", origin), zap.Int32("to", desiredReplicas), zap.String("namespace", vertex.Namespace), zap.String("pipeline", vertex.Spec.PipelineName), zap.String("vertex", vertex.Spec.Name))
	return nil
}

// KeyOfVertex returns the unique key of a vertex
func KeyOfVertex(vertex dfv1.Vertex) string {
	return fmt.Sprintf("%s/%s", vertex.Namespace, vertex.Name)
}

func getBufferInfos(
	ctx context.Context,
	d daemonclient.DaemonClient,
	pl *dfv1.Pipeline,
	vertex *dfv1.Vertex,
) (
	bufferLength int64,
	bufferLengthLimit int64,
	targetAvailableBufferLength int64,
	totalBufferLengthLimit int64,
	totalCurrentPending int64,
	err error,
) {
	bufferLength = int64(0)
	bufferLengthLimit = int64(0)
	targetAvailableBufferLength = int64(0)
	totalBufferLengthLimit = int64(0)
	totalCurrentPending = int64(0)
	for _, bufferName := range vertex.OwnedBuffers() {
		if bInfo, err := d.GetPipelineBuffer(ctx, pl.Name, bufferName); err != nil {
			err = fmt.Errorf("failed to get the buffer information of vertex %q, %w", vertex.Name, err)
			return bufferLength, bufferLengthLimit, targetAvailableBufferLength, totalBufferLengthLimit, totalCurrentPending, err
		} else {
			if bInfo.BufferLength == nil || bInfo.BufferUsageLimit == nil || bInfo.PendingCount == nil {
				err = fmt.Errorf("invalid read buffer information of vertex %q, length, pendingCount or usage limit is missing", vertex.Name)
				return bufferLength, bufferLengthLimit, targetAvailableBufferLength, totalBufferLengthLimit, totalCurrentPending, err
			}
			bufferLength = bInfo.GetBufferLength().GetValue()
			bufferLengthLimit = int64(float64(bInfo.GetBufferLength().GetValue()) * bInfo.GetBufferUsageLimit().GetValue())
			targetAvailableBufferLength = int64(float64(bInfo.GetBufferLength().GetValue()) * float64(vertex.Spec.Scale.GetTargetBufferAvailability()) / 100)
			totalBufferLengthLimit += int64(float64(bInfo.BufferLength.GetValue()) * bInfo.BufferUsageLimit.GetValue())
			totalCurrentPending += bInfo.PendingCount.GetValue()
		}
	}

	return bufferLength, bufferLengthLimit, targetAvailableBufferLength, totalBufferLengthLimit, totalCurrentPending, nil
}
