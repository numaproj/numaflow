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
	mvtxdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type Scaler struct {
	client     client.Client
	monoVtxMap map[string]*list.Element
	// List of the mono vertex namespaced name, format is "namespace/name"
	monoVtxList *list.List
	lock        *sync.RWMutex
	options     *options
	// Cache to store the vertex metrics such as pending message number
	monoVtxMetricsCache    *lru.Cache[string, int64]
	mvtxDaemonClientsCache *lru.Cache[string, mvtxdaemonclient.MonoVertexDaemonClient]
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
		client:      client,
		options:     scalerOpts,
		monoVtxMap:  make(map[string]*list.Element),
		monoVtxList: list.New(),
		lock:        new(sync.RWMutex),
	}
	// cache the clients
	s.mvtxDaemonClientsCache, _ = lru.NewWithEvict[string, mvtxdaemonclient.MonoVertexDaemonClient](s.options.clientsCacheSize, func(key string, value mvtxdaemonclient.MonoVertexDaemonClient) {
		_ = value.Close()
	})
	monoVtxMetricsCache, _ := lru.New[string, int64](10000)
	s.monoVtxMetricsCache = monoVtxMetricsCache
	return s
}

// Contains returns if the Scaler contains the key.
func (s *Scaler) Contains(key string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.monoVtxMap[key]
	return ok
}

// Length returns how many vertices are being watched for autoscaling
func (s *Scaler) Length() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.monoVtxList.Len()
}

// StartWatching put a key (namespace/name) into the Scaler
func (s *Scaler) StartWatching(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.monoVtxMap[key]; !ok {
		s.monoVtxMap[key] = s.monoVtxList.PushBack(key)
	}
}

// StopWatching stops autoscaling on the key (namespace/name)
func (s *Scaler) StopWatching(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if e, ok := s.monoVtxMap[key]; ok {
		_ = s.monoVtxList.Remove(e)
		delete(s.monoVtxMap, key)
	}
}

// Function scale() defines each of the worker's job.
// It waits for keys in the channel, and starts a scaling job
func (s *Scaler) scale(ctx context.Context, id int, keyCh <-chan string) {
	log := logging.FromContext(ctx)
	log.Infof("Started MonoVertex autoscaling worker %v", id)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopped MonoVertex autoscaling worker %v", id)
			return
		case key := <-keyCh:
			if err := s.scaleOneMonoVertex(ctx, key, id); err != nil {
				log.Errorw("Failed to scale a MonoVertex", zap.String("monoVtx", key), zap.Error(err))
			}
		}
	}
}

// scaleOneMonoVertex implements the detailed logic of scaling up/down a MonoVertex.
//
//	desiredReplicas = currentReplicas * pending / (targetProcessingTime * rate)
func (s *Scaler) scaleOneMonoVertex(ctx context.Context, key string, worker int) error {
	log := logging.FromContext(ctx).With("worker", fmt.Sprint(worker)).With("monoVtxKey", key)
	log.Debugf("Working on key: %s.", key)
	strs := strings.Split(key, "/")
	if len(strs) != 2 {
		return fmt.Errorf("invalid key %q", key)
	}
	namespace := strs[0]
	monoVtxName := strs[1]
	monoVtx := &dfv1.MonoVertex{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: monoVtxName}, monoVtx); err != nil {
		if apierrors.IsNotFound(err) {
			s.StopWatching(key)
			log.Info("No corresponding MonoVertex found, stopped watching.")
			return nil
		}
		return fmt.Errorf("failed to query MonoVertex object of key %q, %w", key, err)
	}
	if !monoVtx.GetDeletionTimestamp().IsZero() {
		s.StopWatching(key)
		log.Debug("MonoVertex being deleted.")
		return nil
	}
	if !monoVtx.Scalable() {
		s.StopWatching(key) // Remove it in case it's watched.
		return nil
	}
	if monoVtx.Status.UpdateHash != monoVtx.Status.CurrentHash && monoVtx.Status.UpdateHash != "" {
		log.Info("MonoVertex is updating, skip scaling.")
		return nil
	}
	secondsSinceLastScale := time.Since(monoVtx.Status.LastScaledAt.Time).Seconds()
	scaleDownCooldown := float64(monoVtx.Spec.Scale.GetScaleDownCooldownSeconds())
	scaleUpCooldown := float64(monoVtx.Spec.Scale.GetScaleUpCooldownSeconds())
	if secondsSinceLastScale < scaleDownCooldown && secondsSinceLastScale < scaleUpCooldown {
		// Skip scaling without needing further calculation
		log.Infof("Cooldown period, skip scaling.")
		return nil
	}
	if monoVtx.Status.Phase != dfv1.MonoVertexPhaseRunning {
		log.Infof("MonoVertex not in Running phase, skip scaling.")
		return nil
	}

	if monoVtx.Spec.Lifecycle.GetDesiredPhase() != dfv1.MonoVertexPhaseRunning {
		log.Info("MonoVertex desiredPhase is not running, skip scaling.")
		return nil
	}
	if int(monoVtx.Status.Replicas) != monoVtx.CalculateReplicas() {
		log.Infof("MonoVertex %s might be under processing, replicas mismatch, skip scaling.", monoVtx.Name)
		return nil
	}

	if monoVtx.Spec.Scale.GetMaxReplicas() == monoVtx.Spec.Scale.GetMinReplicas() {
		log.Infof("MonoVertex %s has same scale.min and scale.max, skip scaling.", monoVtx.Name)
		return nil
	}

	if monoVtx.Status.Replicas == 0 { // Was scaled to 0
		// Periodically wake them up from 0 replicas to 1, to peek for the incoming messages
		if secondsSinceLastScale >= float64(monoVtx.Spec.Scale.GetZeroReplicaSleepSeconds()) {
			log.Infof("MonoVertex %s has slept %v seconds, scaling up to peek.", monoVtx.Name, secondsSinceLastScale)
			return s.patchMonoVertexReplicas(ctx, monoVtx, 1)
		} else {
			log.Infof("MonoVertex %q has slept %v seconds, hasn't reached zeroReplicaSleepSeconds (%v seconds), skip scaling.", monoVtx.Name, secondsSinceLastScale, monoVtx.Spec.Scale.GetZeroReplicaSleepSeconds())
			return nil
		}
	}

	// There's no ready pods, skip scaling
	if monoVtx.Status.ReadyReplicas == 0 {
		log.Infof("MonoVertex has no ready replicas, skip scaling.")
		return nil
	}

	var err error
	daemonClient, _ := s.mvtxDaemonClientsCache.Get(monoVtx.GetDaemonServiceURL())
	if daemonClient == nil {
		daemonClient, err = mvtxdaemonclient.NewGRPCClient(monoVtx.GetDaemonServiceURL())
		if err != nil {
			return fmt.Errorf("failed to get daemon service client for MonoVertex %s, %w", monoVtx.Name, err)
		}
		s.mvtxDaemonClientsCache.Add(monoVtx.GetDaemonServiceURL(), daemonClient)
	}

	vMetrics, err := daemonClient.GetMonoVertexMetrics(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metrics of mono vertex key %q, %w", key, err)
	}
	totalRate := float64(0)
	totalPending := int64(0)
	rate, existing := vMetrics.ProcessingRates["default"]
	// If rate is not available, we skip scaling.
	if !existing || rate.GetValue() < 0 { // Rate not available
		log.Infof("MonoVertex %s has no rate information, skip scaling.", monoVtxName)
		return nil
	} else {
		totalRate = rate.GetValue()
	}
	pending, existing := vMetrics.Pendings["default"]
	if !existing || pending.GetValue() < 0 || pending.GetValue() == dfv1.PendingNotAvailable {
		// Pending not available, we don't do anything
		log.Infof("MonoVertex %s has no pending messages information, skip scaling.", monoVtxName)
		return nil
	} else {
		totalPending = pending.GetValue()
	}
	desired := s.desiredReplicas(ctx, monoVtx, totalRate, totalPending)

	// Check if rate limiting is configured and we're hitting the limit
	// For MonoVertex, the rate limit is defined at Source by how many times the `Read` is called per second multiplied
	// by the `readBatchSize`.
	if monoVtx.Spec.Limits != nil && monoVtx.Spec.Limits.RateLimit != nil && monoVtx.Spec.Limits.RateLimit.Max != nil {
		maxRate := float64(*monoVtx.Spec.Limits.RateLimit.Max * monoVtx.Spec.Limits.GetReadBatchSize())
		// Round up to the nearest integer because we don't want to scale up if we are almost at the rate limit
		// e.g., RateLimit is 1000, current rate is 999.9, we don't want to scale up
		currentRate := math.Ceil(totalRate)

		if currentRate >= maxRate {
			// Calculate desired replicas to see if we would scale up
			current := int32(monoVtx.Status.Replicas)

			// Only skip if we would be scaling up
			if desired > current {
				log.Infof("MonoVertex %s would scale up but is at rate limit, skip scaling up.", monoVtxName)
				return nil
			}
		}
	}

	log.Infof("Calculated desired replica number of MonoVertex %q is: %d.", monoVtx.Name, desired)
	maxReplicas := monoVtx.Spec.Scale.GetMaxReplicas()
	minReplicas := monoVtx.Spec.Scale.GetMinReplicas()
	if desired > maxReplicas {
		desired = maxReplicas
		log.Infof("Calculated desired replica number %d of MonoVertex %q is greater than max, using max %d.", desired, monoVtxName, maxReplicas)
	}
	if desired < minReplicas {
		desired = minReplicas
		log.Infof("Calculated desired replica number %d of MonoVertex %q is smaller than min, using min %d.", desired, monoVtxName, minReplicas)
	}
	current := int32(monoVtx.Status.Replicas)
	if current > maxReplicas || current < minReplicas { // Someone might have manually scaled up/down the MonoVertex
		return s.patchMonoVertexReplicas(ctx, monoVtx, desired)
	}
	if desired < current {
		maxAllowedDown := int32(monoVtx.Spec.Scale.GetReplicasPerScaleDown())
		diff := current - desired
		if diff > maxAllowedDown {
			diff = maxAllowedDown
		}
		if secondsSinceLastScale < scaleDownCooldown {
			log.Infof("Cooldown period for scaling down, skip scaling.")
			return nil
		}
		return s.patchMonoVertexReplicas(ctx, monoVtx, current-diff) // We scale down gradually
	}
	if desired > current {
		maxAllowedUp := int32(monoVtx.Spec.Scale.GetReplicasPerScaleUp())
		diff := desired - current
		if diff > maxAllowedUp {
			diff = maxAllowedUp
		}
		if secondsSinceLastScale < scaleUpCooldown {
			log.Infof("Cooldown period for scaling up, skip scaling.")
			return nil
		}
		return s.patchMonoVertexReplicas(ctx, monoVtx, current+diff) // We scale up gradually
	}
	return nil
}

func (s *Scaler) desiredReplicas(_ context.Context, monoVtx *dfv1.MonoVertex, processingRate float64, pending int64) int32 {
	// Since pending contains the pending acks, if both totalRate and totalPending are 0, we scale down to 0
	if pending == 0 && processingRate == 0 {
		return 0
	}
	if processingRate == 0 { // Something is wrong, we don't do anything.
		return int32(monoVtx.Status.Replicas)
	}

	var desired int32
	// We calculate the time of finishing processing the pending messages,
	// and then we know how many replicas are needed to get them done in target seconds.
	desired = int32(math.Round(((float64(pending) / processingRate) / float64(monoVtx.Spec.Scale.GetTargetProcessingSeconds())) * float64(monoVtx.Status.ReadyReplicas)))

	// we only scale down to zero when the pending and rate are both zero.
	if desired == 0 {
		desired = 1
	}
	if desired > int32(pending) && pending > 0 { // For some corner cases, we don't want to scale up to more than pending.
		desired = int32(pending)
	}
	return desired
}

// Start function starts the autoscaling worker group.
// Each worker keeps picking up scaling tasks (which contains mono vertex keys) to calculate the desired replicas,
// and patch the mono vertex spec with the new replica number if needed.
func (s *Scaler) Start(ctx context.Context) error {
	log := logging.FromContext(ctx).Named("mvtx-autoscaler")
	log.Info("Starting MonoVertex autoscaler...")
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
		if s.monoVtxList.Len() == 0 {
			return
		}
		e := s.monoVtxList.Front()
		if key, ok := e.Value.(string); ok {
			s.monoVtxList.MoveToBack(e)
			keyCh <- key
		}
	}

	// Following for loop keeps calling assign() function to assign scaling tasks to the workers.
	// It makes sure each element in the list will be assigned every N milliseconds.
	for {
		select {
		case <-ctx.Done():
			log.Info("Shutting down mono vertex autoscaling job assigner.")
			// clear the daemon clients cache
			s.mvtxDaemonClientsCache.Purge()
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

func (s *Scaler) patchMonoVertexReplicas(ctx context.Context, monoVtx *dfv1.MonoVertex, desiredReplicas int32) error {
	log := logging.FromContext(ctx)
	origin := monoVtx.Spec.Replicas
	patchJson := fmt.Sprintf(`{"spec":{"replicas":%d}}`, desiredReplicas)
	if err := s.client.Patch(ctx, monoVtx, client.RawPatch(types.MergePatchType, []byte(patchJson))); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to patch MonoVertex replicas, %w", err)
	}
	log.Infow("Auto scaling - mono vertex replicas changed.", zap.Int32p("from", origin), zap.Int32("to", desiredReplicas), zap.String("namespace", monoVtx.Namespace), zap.String("vertex", monoVtx.Name))
	return nil
}

// KeyOfMonoVertex returns the unique key of a MonoVertex
func KeyOfMonoVertex(monoVtx dfv1.MonoVertex) string {
	return fmt.Sprintf("%s/%s", monoVtx.Namespace, monoVtx.Name)
}
