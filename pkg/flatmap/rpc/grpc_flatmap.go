package rpc

import (
	"fmt"
	"log"
	"time"

	flatmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/flatmap/v1"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/numaproj/numaflow/pkg/flatmap/tracker"
	"github.com/numaproj/numaflow/pkg/flatmap/types"
	"github.com/numaproj/numaflow/pkg/isb"
	sdkerr "github.com/numaproj/numaflow/pkg/sdkclient/error"
	"github.com/numaproj/numaflow/pkg/sdkclient/flatmapper"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// GRPCBasedFlatmap is a flat map applier that uses gRPC client to invoke the flat map UDF.
// It implements the applier.FlatmapApplier interface.
type GRPCBasedFlatmap struct {
	client        flatmapper.Client
	tracker       *tracker.Tracker
	readBatchSize int
	idx           int
}

func NewUDSgRPCBasedFlatmap(client flatmapper.Client, batchSize int) *GRPCBasedFlatmap {
	return &GRPCBasedFlatmap{client: client, tracker: tracker.NewTracker(), readBatchSize: batchSize}
}

// IsHealthy checks if the map udf is healthy.
func (u *GRPCBasedFlatmap) IsHealthy(ctx context.Context) error {
	return u.WaitUntilReady(ctx)
}

// CloseConn closes the gRPC client connection.
func (u *GRPCBasedFlatmap) CloseConn(ctx context.Context) error {
	return u.client.CloseConn(ctx)
}

// WaitUntilReady waits until the reduce udf is connected.
func (u *GRPCBasedFlatmap) WaitUntilReady(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed on readiness check: %w", ctx.Err())
		default:
			if _, err := u.client.IsReady(ctx, &emptypb.Empty{}); err == nil {
				return nil
			} else {
				logger.Infof("waiting for reduce udf to be ready: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (u *GRPCBasedFlatmap) ApplyMap(ctx context.Context, messageStream []*isb.ReadMessage, responseCh chan<- *types.ResponseFlatmap) <-chan error {
	var (
		errCh = make(chan error)
	)
	flatmapRequests := make(chan *flatmappb.MapRequest)
	defer close(flatmapRequests)

	// invoke the AsyncReduceFn method with requestCh channel and send the result to responseCh channel
	// and any error to errCh channel
	go func() {
		// TODO(stream): check this close here
		defer close(responseCh)
		resultCh, reduceErrCh := u.client.MapFn(ctx, flatmapRequests)
		for {
			select {
			case result, ok := <-resultCh:
				if !ok || result == nil {
					//// if the resultCh channel is closed, close the responseCh
					return
				}
				resp, remove := u.parseMapResponse(result)
				if remove {
					u.tracker.RemoveRequest(resp.Uid)
					continue
				}
				responseCh <- resp
				log.Println("MYDEBUG: sending to writeCh", resp.Uid)
			case err := <-reduceErrCh:
				// ctx.Done() event will be handled by the AsyncReduceFn method
				// so we don't need a separate case for ctx.Done() here
				if err == ctx.Err() {
					errCh <- err
					return
				}
				if err != nil {
					errCh <- convertToUdfError(err)
					// TODO(stream): check error here
				}
			}
		}
	}()

	for _, req := range messageStream {
		d := u.createMapRequest(req)
		log.Print("MYDEBUG: TRYING TO SEND HERE", d.Uuid)
		flatmapRequests <- d
	}

	return errCh
}

func (u *GRPCBasedFlatmap) createMapRequest(msg *isb.ReadMessage) *flatmappb.MapRequest {
	keys := msg.Keys
	payload := msg.Body.Payload

	uid := u.tracker.AddRequest(msg)

	var d = &flatmappb.MapRequest{
		Keys:      keys,
		Value:     payload,
		EventTime: timestamppb.New(msg.EventTime),
		Watermark: timestamppb.New(msg.Watermark),
		Headers:   msg.Headers,
		Uuid:      uid,
	}
	return d
}
func (u *GRPCBasedFlatmap) parseMapResponse(resp *flatmappb.MapResponse) (parsedResp *types.ResponseFlatmap, requestDone bool) {
	result := resp.Result
	eor := result.GetEOR()
	uid := result.GetUuid()
	parentRequest, ok := u.tracker.GetRequest(uid)
	// TODO(stream): check what should be path for !ok
	if !ok {
		//	u.tracker.NewResponse(uid)
		//	idx = 1
	}
	// Request has completed remove from the tracker module
	if eor == true {
		return nil, true
	}
	//idx, present := u.tracker.GetIdx(uid)
	//if !present {

	//}
	keys := result.GetKeys()
	taggedMessage := &isb.WriteMessage{
		Message: isb.Message{
			Header: isb.Header{
				MessageInfo: parentRequest.MessageInfo,
				// TODO(stream): Check what will be the unique ID to use here
				//msgId := fmt.Sprintf("%s-%d-%s-%d", u.vertexName, u.vertexReplica, partitionID.String(), index)
				//ID: fmt.Sprintf("%s-%s-%d", dataMessages[0].ReadOffset.String(), isdf.vertexName, msgIndex)
				ID:   fmt.Sprintf("%s-%d", parentRequest.ReadOffset.String(), u.idx),
				Keys: keys,
			},
			Body: isb.Body{
				Payload: result.GetValue(),
			},
		},

		Tags: result.GetTags(),
	}

	u.idx += 1
	//u.tracker.IncrementRespIdx(uid)
	return &types.ResponseFlatmap{
		ParentMessage: parentRequest,
		Uid:           uid,
		RespMessage:   taggedMessage,
	}, false
}

// convertToUdfError converts the error returned by the reduceFn to ApplyUDFErr
func convertToUdfError(err error) error {
	// if any error happens in reduce
	// will exit and restart the numa container
	udfErr, _ := sdkerr.FromError(err)
	switch udfErr.ErrorKind() {
	case sdkerr.Retryable:
		// TODO: currently we don't handle retryable errors for reduce
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	case sdkerr.NonRetryable:
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	case sdkerr.Canceled:
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    context.Canceled.Error(),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	default:
		return &ApplyUDFErr{
			UserUDFErr: false,
			Message:    fmt.Sprintf("gRPC client.ReduceFn failed, %s", err),
			InternalErr: InternalErr{
				Flag:        true,
				MainCarDown: false,
			},
		}
	}
}
