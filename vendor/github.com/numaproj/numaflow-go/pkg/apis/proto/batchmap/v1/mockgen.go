package v1

//go:generate mockgen -destination batchmapmock/batchmapmock.go -package batchmapmock github.com/numaproj/numaflow-go/pkg/apis/proto/batchmap/v1 BatchMapClient,BatchMap_BatchMapFnClient
