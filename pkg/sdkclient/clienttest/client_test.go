package clienttest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	functionpb "github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1"
	"github.com/numaproj/numaflow-go/pkg/apis/proto/function/v1/funcmock"
)

type rpcMsg struct {
	msg proto.Message
}

func (r *rpcMsg) Matches(msg interface{}) bool {
	m, ok := msg.(proto.Message)
	if !ok {
		return false
	}
	return proto.Equal(m, r.msg)
}

func (r *rpcMsg) String() string {
	return fmt.Sprintf("is %s", r.msg)
}

func TestIsReady(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&functionpb.ReadyResponse{Ready: true}, nil)
	mockClient.EXPECT().IsReady(gomock.Any(), gomock.Any()).Return(&functionpb.ReadyResponse{Ready: false}, fmt.Errorf("mock connection refused"))

	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	ready, err := testClient.IsReady(ctx, &emptypb.Empty{})
	assert.True(t, ready)
	assert.NoError(t, err)

	ready, err = testClient.IsReady(ctx, &emptypb.Empty{})
	assert.False(t, ready)
	assert.EqualError(t, err, "mock connection refused")
}

func TestMapFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	testRequestDatum := &functionpb.DatumRequest{
		Keys:      []string{"test_success_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}
	testResponseDatum := &functionpb.DatumResponse{
		Keys:      []string{"test_success_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}
	mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: testRequestDatum}).Return(&functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			testResponseDatum,
		},
	}, nil)
	mockClient.EXPECT().MapFn(gomock.Any(), &rpcMsg{msg: testRequestDatum}).Return(&functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			nil,
		},
	}, fmt.Errorf("mock MapFn error"))
	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})
	got, err := testClient.MapFn(ctx, testRequestDatum)
	reflect.DeepEqual(got, testRequestDatum)
	assert.NoError(t, err)

	got, err = testClient.MapFn(ctx, testRequestDatum)
	assert.Nil(t, got)
	assert.EqualError(t, err, "NonRetryable: mock MapFn error")
}

func TestMapStreamFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockMapStreamClient := funcmock.NewMockUserDefinedFunction_MapStreamFnClient(ctrl)

	requestDatum := &functionpb.DatumRequest{
		Keys:      []string{"test_success_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}
	expectedDatum := &functionpb.DatumResponse{
		Keys:      []string{"test_success_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}

	mockMapStreamClient.EXPECT().Recv().Return(expectedDatum, nil).Times(1)
	mockMapStreamClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

	mockClient.EXPECT().MapStreamFn(gomock.Any(), &rpcMsg{msg: requestDatum}).Return(mockMapStreamClient, nil)

	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	datumCh := make(chan *functionpb.DatumResponse)
	datumResponses := make([]*functionpb.DatumResponse, 0)

	go func() {
		err := testClient.MapStreamFn(ctx, requestDatum, datumCh)
		assert.NoError(t, err)
	}()

	for msg := range datumCh {
		datumResponses = append(datumResponses, msg)
	}
	assert.True(t, reflect.DeepEqual(datumResponses, []*functionpb.DatumResponse{expectedDatum}))
}

func TestMapStreamFnErr(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockMapStreamClient := funcmock.NewMockUserDefinedFunction_MapStreamFnClient(ctrl)

	requestDatum := &functionpb.DatumRequest{
		Keys:      []string{"test_error_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}
	expectedDatum := &functionpb.DatumResponse{
		Keys:      []string{"test_error_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}

	mockMapStreamClient.EXPECT().Recv().Return(expectedDatum, nil).Times(1)
	mockMapStreamClient.EXPECT().Recv().Return(
		nil, errors.New("mock error for map")).AnyTimes()

	mockClient.EXPECT().MapStreamFn(gomock.Any(), &rpcMsg{msg: requestDatum}).Return(mockMapStreamClient, nil)

	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	datumCh := make(chan *functionpb.DatumResponse)
	errs, ctx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		return testClient.MapStreamFn(ctx, requestDatum, datumCh)
	})

	datumResponses := make([]*functionpb.DatumResponse, 0)
	for msg := range datumCh {
		datumResponses = append(datumResponses, msg)
	}

	err = errs.Wait()
	assert.Error(t, err)
}

func TestMapTFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	testDatumRequest := &functionpb.DatumRequest{
		Keys:      []string{"test_success_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}
	testDatumResponse := &functionpb.DatumResponse{
		Keys:      []string{"test_success_key"},
		Value:     []byte(`forward_message`),
		EventTime: &functionpb.EventTime{EventTime: timestamppb.New(time.Unix(1661169600, 0))},
		Watermark: &functionpb.Watermark{Watermark: timestamppb.New(time.Time{})},
	}
	mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: testDatumRequest}).Return(&functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			testDatumResponse,
		},
	}, nil)
	mockClient.EXPECT().MapTFn(gomock.Any(), &rpcMsg{msg: testDatumRequest}).Return(&functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			nil,
		},
	}, fmt.Errorf("mock MapTFn error"))
	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})
	got, err := testClient.MapTFn(ctx, testDatumRequest)
	reflect.DeepEqual(got, testDatumRequest)
	assert.NoError(t, err)

	got, err = testClient.MapTFn(ctx, testDatumRequest)
	assert.Nil(t, got)
	assert.EqualError(t, err, "NonRetryable: mock MapTFn error")
}

func TestReduceFn(t *testing.T) {
	var ctx = context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := funcmock.NewMockUserDefinedFunctionClient(ctrl)
	mockReduceClient := funcmock.NewMockUserDefinedFunction_ReduceFnClient(ctrl)

	mockReduceClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockReduceClient.EXPECT().CloseSend().Return(nil).AnyTimes()

	testDatumList := &functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}

	mockReduceClient.EXPECT().Recv().Return(testDatumList, nil).Times(3)

	mockReduceClient.EXPECT().Recv().Return(nil, io.EOF).Times(1)

	datumCh := make(chan *functionpb.DatumRequest)

	mockClient.EXPECT().ReduceFn(gomock.Any(), gomock.Any()).Return(mockReduceClient, nil)

	testClient, err := New(mockClient)
	assert.NoError(t, err)
	reflect.DeepEqual(testClient, &client{
		grpcClt: mockClient,
	})

	expectedDatumList := &functionpb.DatumResponseList{
		Elements: []*functionpb.DatumResponse{
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
			{
				Keys:  []string{"reduced_result_key"},
				Value: []byte(`forward_message`),
			},
		},
	}
	close(datumCh)
	got, err := testClient.ReduceFn(ctx, datumCh)
	assert.True(t, reflect.DeepEqual(got, expectedDatumList.Elements))
	assert.NoError(t, err)
}
