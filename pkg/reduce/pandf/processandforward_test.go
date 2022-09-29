package pandf

import (
	"context"
	"github.com/golang/mock/gomock"
	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
	"github.com/numaproj/numaflow-go/pkg/function/clienttest"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	udfcall "github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/window/keyed"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestProcessAndForward_Process(t *testing.T) {
	// 1. create a pbq which has to be passed to the process method
	// 2. pass the pbqReader interface and create a new p and f instance
	// 3. mock the grpc client methods
	// 4. assert to check if the process method is returning the result
	// 5. assert to check if the persisted messages is deleted from the store

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			t.Log(t.Name(), "test timeout")
		}
	}()

	size := 100
	testPartition := keyed.PartitionID{
		Start: time.Now(),
		End:   time.Now(),
		Key:   "partition-1",
	}
	var err error
	var pbqManager *pbq.Manager

	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithStoreSize(int64(size)), store.WithPbqStoreType(dfv1.InMemoryType)),
		pbq.WithReadTimeout(1*time.Second), pbq.WithChannelBufferSize(10))
	assert.NoError(t, err)

	// create a pbq for a partition
	var simplePbq pbq.ReadWriteCloser
	simplePbq, err = pbqManager.CreateNewPBQ(ctx, testPartition)
	assert.NoError(t, err)

	// write messages to pbq
	go func() {
		writeMessages := testutils.BuildTestWriteMessages(10, time.Now())
		for index, _ := range writeMessages {
			err := simplePbq.Write(ctx, &writeMessages[index])
			assert.NoError(t, err)
		}
		// done writing, cob
		simplePbq.CloseOfBook()
	}()

	// mock grpc reducer
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockReduceClient := udfcall.NewMockUserDefinedFunction_ReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseAndRecv().Return(&functionpb.DatumList{
		Elements: []*functionpb.Datum{
			{
				Key:   "reduced_result_key",
				Value: []byte(`forward_message`),
			},
		},
	}, nil)

	mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	c, _ := clienttest.New(mockClient)
	client := udfcall.NewUdsGRPCBasedUDFWithClient(c)

	assert.NoError(t, err)

	// create pf using key and reducer
	prfd := NewProcessAndForward(testPartition, client, simplePbq)

	err = prfd.Process(ctx)
	assert.NoError(t, err)
	assert.Len(t, prfd.result, 1)
}
