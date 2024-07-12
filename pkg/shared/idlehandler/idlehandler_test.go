package idlehandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zaptest"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// Define a mock BufferWriter
type MockBufferWriter struct {
	mock.Mock
}

func (m *MockBufferWriter) GetName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockBufferWriter) GetPartitionIdx() int32 {
	args := m.Called()
	return args.Get(0).(int32)
}

func (m *MockBufferWriter) Write(ctx context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	args := m.Called(ctx, messages)
	return args.Get(0).([]isb.Offset), args.Get(1).([]error)
}

func (m *MockBufferWriter) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Define a mock Publisher
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) PublishWatermark(wm wmb.Watermark, offset isb.Offset, partition int32) {
	m.Called(wm, offset, partition)
}

func (m *MockPublisher) PublishIdleWatermark(wm wmb.Watermark, offset isb.Offset, partition int32) {
	m.Called(wm, offset, partition)
}

func (m *MockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPublisher) GetLatestWatermark() wmb.Watermark {
	args := m.Called()
	return args.Get(0).(wmb.Watermark)
}

// Define a mock IdleManager
type MockIdleManager struct {
	mock.Mock
}

func (m *MockIdleManager) MarkIdle(fromBufferPartitionIndex int32, toPartitionName string) {
	m.Called(fromBufferPartitionIndex, toPartitionName)
}

func (m *MockIdleManager) NeedToSendCtrlMsg(toPartitionName string) bool {
	args := m.Called(toPartitionName)
	return args.Bool(0)
}

func (m *MockIdleManager) Update(fromBufferPartitionIndex int32, toPartitionName string, offset isb.Offset) {
	m.Called(fromBufferPartitionIndex, toPartitionName, offset)
}

func (m *MockIdleManager) Get(toPartitionName string) isb.Offset {
	args := m.Called(toPartitionName)
	return args.Get(0).(isb.Offset)
}

func (m *MockIdleManager) MarkActive(fromBufferPartitionIndex int32, toPartitionName string) {
	m.Called(fromBufferPartitionIndex, toPartitionName)
}

// Unit test the PublishIdleWatermark function
func TestPublishIdleWatermark_SinkVertex(t *testing.T) {
	ctx := context.Background()
	fromBufferPartitionIndex := int32(1)
	vertexName := "test-vertex"
	pipelineName := "test-pipeline"
	vertexType := dfv1.VertexTypeSink
	vertexReplica := int32(0)
	wm := wmb.Watermark{}

	logger := zaptest.NewLogger(t).Sugar()

	toBufferPartition := new(MockBufferWriter)
	toBufferPartition.On("GetName").Return("test-partition")
	toBufferPartition.On("GetPartitionIdx").Return(int32(0))

	wmPublisher := new(MockPublisher)
	idleManager := new(MockIdleManager)

	idleManager.On("MarkIdle", fromBufferPartitionIndex, "test-partition").Return()
	idleManager.On("NeedToSendCtrlMsg", "test-partition").Return(false)
	wmPublisher.On("PublishIdleWatermark", wm, nil, int32(0)).Return()
	wmPublisher.On("GetLatestWatermark").Return(wm)

	PublishIdleWatermark(ctx, fromBufferPartitionIndex, toBufferPartition, wmPublisher, idleManager, logger, vertexName, pipelineName, vertexType, vertexReplica, wm)

	// Verify that the mock methods are called as expected
	idleManager.AssertCalled(t, "MarkIdle", fromBufferPartitionIndex, "test-partition")
	// idleManager.AssertNotCalled(t, "NeedToSendCtrlMsg", "test-partition")
	wmPublisher.AssertCalled(t, "PublishIdleWatermark", wm, nil, int32(0))
}
