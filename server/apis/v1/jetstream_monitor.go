/*
Copyright 2026 The Numaproj Authors.

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

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	natsserver "github.com/nats-io/nats-server/v2/server"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

const jetStreamMonitorPort = 8222

func (h *handler) GetISBServiceJetStream(c *gin.Context) {
	ns, isbsvcName := c.Param("namespace"), c.Param("isb-service")
	_, pods, ok := h.getJetStreamISBMonitorPods(c, ns, isbsvcName)
	if !ok {
		return
	}
	response := ISBJetStreamDTO{
		Summary:       []JetStreamSummaryDTO{},
		RaftMetaGroup: []JetStreamRaftMetaDTO{},
	}
	for _, pod := range pods {
		jsInfo, err := h.fetchJetStreamInfo(c.Request.Context(), pod)
		if err != nil {
			response.Errors = append(response.Errors, ISBMonitorErrorDTO{Pod: pod.Name, Message: err.Error()})
			continue
		}
		response.Summary = append(response.Summary, newJetStreamSummaryDTO(pod.Name, jsInfo))
		response.RaftMetaGroup = append(response.RaftMetaGroup, newJetStreamRaftMetaDTOs(jsInfo)...)
	}
	if len(response.Summary) == 0 && len(response.Errors) > 0 {
		message := fmt.Sprintf("Failed to fetch JetStream monitor data for interstepbuffer service %q namespace %q", isbsvcName, ns)
		c.JSON(http.StatusBadGateway, NewNumaflowAPIResponse(&message, nil))
		return
	}
	response.RaftMetaGroup = dedupeJetStreamRaftMetaDTOs(response.RaftMetaGroup)
	sort.Slice(response.Summary, func(i, j int) bool {
		return response.Summary[i].Server < response.Summary[j].Server
	})
	sort.Slice(response.RaftMetaGroup, func(i, j int) bool {
		if response.RaftMetaGroup[i].Name != response.RaftMetaGroup[j].Name {
			return response.RaftMetaGroup[i].Name < response.RaftMetaGroup[j].Name
		}
		return response.RaftMetaGroup[i].ID < response.RaftMetaGroup[j].ID
	})
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, response))
}

func (h *handler) getJetStreamISBMonitorPods(c *gin.Context, ns, isbsvcName string) (*dfv1.InterStepBufferService, []corev1.Pod, bool) {
	isbsvc, err := h.numaflowClient.InterStepBufferServices(ns).Get(c, isbsvcName, metav1.GetOptions{})
	if err != nil {
		message := fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %s", isbsvcName, ns, err.Error())
		c.JSON(http.StatusNotFound, NewNumaflowAPIResponse(&message, nil))
		return nil, nil, false
	}
	if isbsvc.Spec.JetStream == nil {
		message := fmt.Sprintf("Interstepbuffer service %q namespace %q is not JetStream backed", isbsvcName, ns)
		c.JSON(http.StatusBadRequest, NewNumaflowAPIResponse(&message, nil))
		return nil, nil, false
	}
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(c, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyComponent, dfv1.ComponentISBSvc, dfv1.KeyISBSvcName, isbsvcName),
	})
	if err != nil {
		message := fmt.Sprintf("Failed to list JetStream pods for interstepbuffer service %q namespace %q, %s", isbsvcName, ns, err.Error())
		c.JSON(http.StatusBadGateway, NewNumaflowAPIResponse(&message, nil))
		return nil, nil, false
	}
	runningPods := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
			runningPods = append(runningPods, pod)
		}
	}
	if len(runningPods) == 0 {
		message := fmt.Sprintf("No running JetStream pods found for interstepbuffer service %q namespace %q", isbsvcName, ns)
		c.JSON(http.StatusNotFound, NewNumaflowAPIResponse(&message, nil))
		return nil, nil, false
	}
	sort.Slice(runningPods, func(i, j int) bool {
		return runningPods[i].Name < runningPods[j].Name
	})
	return isbsvc, runningPods, true
}

func (h *handler) fetchJetStreamInfo(ctx context.Context, pod corev1.Pod) (*natsserver.JSInfo, error) {
	client := h.httpClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d/jsz?accounts=true&streams=true&consumers=true&config=true", pod.Status.PodIP, jetStreamMonitorPort), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("JetStream monitor returned status %d", resp.StatusCode)
	}
	var jsInfo natsserver.JSInfo
	if err = json.NewDecoder(resp.Body).Decode(&jsInfo); err != nil {
		return nil, fmt.Errorf("failed to decode JetStream monitor response: %w", err)
	}
	return &jsInfo, nil
}

func newJetStreamSummaryDTO(defaultServer string, jsInfo *natsserver.JSInfo) JetStreamSummaryDTO {
	clusterName := ""
	metaLeader := false
	if jsInfo.Meta != nil {
		clusterName = jsInfo.Meta.Name
		metaLeader = jsInfo.Meta.Leader == defaultServer || jsInfo.Meta.Peer == jsInfo.ID
	}
	apiErrorRate := 0.0
	if jsInfo.API.Total > 0 {
		apiErrorRate = float64(jsInfo.API.Errors) / float64(jsInfo.API.Total)
	}
	return JetStreamSummaryDTO{
		Server:       defaultServer,
		ServerID:     jsInfo.ID,
		Cluster:      clusterName,
		Streams:      jsInfo.Streams,
		Consumers:    jsInfo.Consumers,
		Messages:     jsInfo.Messages,
		Bytes:        jsInfo.Bytes,
		APIRequests:  jsInfo.API.Total,
		APIErrors:    jsInfo.API.Errors,
		APIErrorRate: apiErrorRate,
		MetaLeader:   metaLeader,
	}
}

func newJetStreamRaftMetaDTOs(jsInfo *natsserver.JSInfo) []JetStreamRaftMetaDTO {
	if jsInfo.Meta == nil {
		return nil
	}
	raftMeta := make([]JetStreamRaftMetaDTO, 0, len(jsInfo.Meta.Replicas)+1)
	seen := make(map[string]struct{})
	if jsInfo.Meta.Leader != "" {
		raftMeta = append(raftMeta, JetStreamRaftMetaDTO{
			Name:    jsInfo.Meta.Leader,
			ID:      jsInfo.Meta.Peer,
			Leader:  true,
			Current: true,
			Online:  true,
		})
		seen[jsInfo.Meta.Leader] = struct{}{}
		if jsInfo.Meta.Peer != "" {
			seen[jsInfo.Meta.Peer] = struct{}{}
		}
	}
	for _, replica := range jsInfo.Meta.Replicas {
		if replica == nil {
			continue
		}
		if _, ok := seen[replica.Name]; ok {
			continue
		}
		if _, ok := seen[replica.Peer]; ok && replica.Peer != "" {
			continue
		}
		raftMeta = append(raftMeta, JetStreamRaftMetaDTO{
			Name:    replica.Name,
			ID:      replica.Peer,
			Leader:  replica.Name == jsInfo.Meta.Leader || replica.Peer == jsInfo.Meta.Peer,
			Current: replica.Current,
			Online:  !replica.Offline,
			Active:  replica.Active.String(),
			Lag:     replica.Lag,
		})
	}
	return raftMeta
}

func dedupeJetStreamRaftMetaDTOs(items []JetStreamRaftMetaDTO) []JetStreamRaftMetaDTO {
	deduped := make([]JetStreamRaftMetaDTO, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := item.ID
		if key == "" {
			key = item.Name
		}
		if key == "" {
			key = fmt.Sprintf("%t/%t/%t/%s/%d", item.Leader, item.Current, item.Online, item.Active, item.Lag)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, item)
	}
	return deduped
}
