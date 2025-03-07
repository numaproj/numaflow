package service

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
)

type PodReplica string

type ErrorDetails struct {
	Container string `json:"container"`
	Timestamp string `json:"timestamp"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details"`
}

type MonoVertexRuntimeService struct {
	mvtxdaemon.UnsafeMonoVertexRuntimeServiceServer
	httpClient *http.Client
	localCache map[PodReplica][]ErrorDetails
	mu         sync.Mutex
}

func NewMonoVertexRuntimeService() *MonoVertexRuntimeService {
	log.Print("starting runtime service!")
	return &MonoVertexRuntimeService{
		localCache: make(map[PodReplica][]ErrorDetails),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
	}
}

func (s *MonoVertexRuntimeService) PersistRuntimeError(ctx context.Context, req *mvtxdaemon.PersistRuntimeErrorRequest) (*mvtxdaemon.PersistRuntimeErrorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cacheKey := PodReplica(req.GetMvtxName() + req.GetReplica())
	_, ok := s.localCache[cacheKey]
	if !ok {
		s.localCache[cacheKey] = []ErrorDetails{}
	}
	log.Print("persisting error in local cache")
	s.localCache[cacheKey] = append(s.localCache[cacheKey], ErrorDetails{
		Container: req.GetContainerName(),
		Timestamp: req.GetTimestamp(),
		Code:      req.GetCode(),
		Message:   req.GetMessage(),
		Details:   req.GetDetails(),
	})
	return &mvtxdaemon.PersistRuntimeErrorResponse{Status: "Error persisted successfully"}, nil
}
