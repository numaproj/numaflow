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

package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

// setWriteInfo is used to update the values of isFull flag and MINID
func (pw *RedisWriter) setWriteInfo(_ context.Context) {
	ctx := redisclient.RedisContext
	pw.updateIsFullAndLag(ctx)
	pw.updateMinId(ctx)
}

// updateIsFullAndLag is used to update the isFull flag using both the buffer usage and the consumer consumerLag.
func (pw *RedisWriter) updateIsFullAndLag(ctx context.Context) {
	labels := map[string]string{"buffer": pw.GetName()}

	consumerLag, err := pw.getConsumerLag(ctx)

	// here if the error is stream not found, we should set the isfull flag as false as write (XADD) creates the stream
	if err != nil && strings.Contains(err.Error(), "stream not found") {
		pw.setIsFull(false)
		pw.setError("stream not found", err)
		isbIsFullErrors.With(labels).Inc()
		return
	}

	if err != nil {
		pw.setIsFull(true)
		pw.setError("error in getConsumerLag", err)
		isbIsFullErrors.With(labels).Inc()
		return
	}

	// consumerLag as metric
	isbConsumerLag.With(labels).Set(float64(consumerLag))

	lagDuration := pw.LagDuration.Milliseconds()

	// if specified consumerLag duration is 0, that means we do not want to use consumerLag to determine isFull.
	// if lagDuration is specified we use that to compare against the consumer consumerLag (lastGenerated - lastDelivered)
	if lagDuration != 0 && consumerLag >= lagDuration {
		pw.log.Infow("Increasing Lag", zap.Int64("increasing consumerLag", consumerLag))
		pw.setIsFull(true)
	}

	usage, err := pw.getUsage(ctx)

	// here if the error is stream not found, we should set the isfull flag as false as write (XADD) creates the stream
	if err != nil && strings.Contains(err.Error(), "stream not found") {
		pw.setIsFull(false)
		pw.setError("stream not found", err)
		isbIsFullErrors.With(labels).Inc()
		return
	}

	if err != nil {
		pw.setIsFull(true)
		pw.setError("updateIsFullAndLag error in getMaxLen", err)
		isbIsFullErrors.With(labels).Inc()
		return
	}
	isbBufferUsage.With(labels).Set(usage)

	if usage >= pw.PartitionUsageLimit {
		pw.log.Infow("usage is greater than bufferUsageLimit", zap.Float64("usage", usage))
		pw.setIsFull(true)
		return
	}

	pw.setIsFull(false)

}

// getUsage is used to obtain the % usage of the buffer
func (pw *RedisWriter) getUsage(ctx context.Context) (float64, error) {
	streamLen, err := pw.getStreamLength(ctx)

	if err != nil {
		return 0.0, err
	}

	// set stream length
	pw.setBufferLength(streamLen)

	maxLen := pw.MaxLength
	var usage = float64(streamLen) / float64(maxLen)

	return usage, err
}

func (pw *RedisWriter) getStreamLength(ctx context.Context) (int64, error) {
	streamLength := pw.Client.XLen(ctx, pw.GetStreamName())

	if streamLength.Err() != nil && streamLength.Err().Error() == "ERR no such key" {
		return 0, fmt.Errorf("stream not found:%w", streamLength.Err())
	}

	if streamLength.Err() != nil {
		return 0, fmt.Errorf("xlen error in getMaxLen:%w", streamLength.Err())
	}

	streamLen := streamLength.Val()

	return streamLen, nil
}

// getConsumerLag is used to check if the consumerLag on the buffer. Lag of even one consumer is considered a consumerLag.
// We obtain the last generated Id for a stream using the XInfo command which is the last id added to the stream,
// We also obtain the list of all the delivered ids for all the different consumer groups.
// We assume that if the difference of the lastGeneratedId and min(lastDelivered) > 1 min there is a consumerLag.
// It is tough to get the exact numbers as per https://github.com/redis/redis/issues/8392
func (pw *RedisWriter) getConsumerLag(ctx context.Context) (int64, error) {
	lastGeneratedId, lastGenerated, err := pw.getLastGenerated(ctx)

	if err != nil {
		return 0, fmt.Errorf("error in getting lastGeneratedId: %w", err)
	}

	lastDeliveredId, lastDelivered, err := pw.getLastDelivered(ctx)

	if err != nil {
		return 0, fmt.Errorf("error in getting lastDeliveredId: %w", err)
	}
	// Set the refresh count error to 0
	pw.PartitionWriteInfo.refreshFullError.Store(0)
	// obtain current consumerLag
	consumerLagDuration := time.UnixMilli(lastGeneratedId).Sub(time.UnixMilli(lastDeliveredId))

	pw.setConsumerLag(consumerLagDuration)

	consumerLag := lastGeneratedId - lastDeliveredId

	// look for exact match between lastGenerated and lastDelivered
	if lastGenerated == lastDelivered {
		pw.setHasUnprocessed(false)
	} else {
		pw.setHasUnprocessed(true)
	}

	return consumerLag, nil

}

// updateMinId is used to set the MINID value to delete all the entries that have been processed and ACKed by the stream
// Pending is always a consequence of last delivered, since we will not have pending entries without it being delivered first,
// So we do a getLastDelivered first, then perform a getPending and if it gives us a value, we can be sure that the pending will
// be the minimal among the two. If we do not see values in getPending we can safely set minId to the lastDeliveredId.

func (pw *RedisWriter) updateMinId(ctx context.Context) {
	_, lastDelivered, err := pw.getLastDelivered(ctx)

	if err != nil {
		pw.setError("Error obtaining lastDeliveredId", err)
	}

	if lastDelivered == "" {
		pw.log.Debugw("No last deliveredId found, leaving minId at 0-0")
		return
	}

	pending, err := pw.getPending(ctx)

	if err != nil {
		pw.setError("Error in getting Pending results", err)
		return
	}

	// set pending count
	pw.setPendingCount(pending.Val().Count)

	// do not see a pending list, keep minId as lastDelivered as we run pending only after last delivered
	if pending.Val().Count == 0 {
		pw.setMinId(lastDelivered)
		return
	}

	minPending := pending.Val().Lower
	pw.setMinId(minPending)
}

// getLastGenerated is used to obtain the last generatedId on the redis stream
func (pw *RedisWriter) getLastGenerated(ctx context.Context) (int64, string, error) {
	var lastGeneratedId int64
	var lastGenerated string
	infoStream := pw.Client.XInfoStream(ctx, pw.GetStreamName())
	// Explicitly set error to nil if there
	if infoStream.Err() != nil && infoStream.Err().Error() == "ERR no such key" {
		return lastGeneratedId, lastGenerated, fmt.Errorf("stream not found: %w", infoStream.Err())
	}

	if infoStream.Err() != nil {
		return lastGeneratedId, lastGenerated, fmt.Errorf("XInfoStream not getConsumerLag: %w", infoStream.Err())
	}

	lastGenerated = infoStream.Val().LastGeneratedID

	lastGeneratedId, err := splitId(lastGenerated)

	if err != nil {
		return lastGeneratedId, lastGenerated, fmt.Errorf("error in getting lastGeneratedId: %w", err)
	}

	return lastGeneratedId, lastGenerated, nil

}

// getLastDelivered is used to get the last delivered Id
// We return the integer and string versions of lastDelivered here because consumer consumerLag expects an integer, whereas
// minId expects a string
func (pw *RedisWriter) getLastDelivered(ctx context.Context) (int64, string, error) {
	var lastDeliveredId int64
	var lastDelivered string
	var err error

	xInfoGroups := pw.Client.XInfoGroups(ctx, pw.GetStreamName())

	if xInfoGroups.Err() != nil {
		pw.setError("XInfoGroup error in getLastDelivered", xInfoGroups.Err())
		return lastDeliveredId, lastDelivered, xInfoGroups.Err()
	}
	results := xInfoGroups.Val()

	// If you do not have a consumer you might keep writing and might run into "NO SPACE LEFT", on contrary, buffer writer won't write unless there is a reader.
	// we return true today, but how should we handle it differently?
	if len(results) == 0 {
		err = fmt.Errorf("no consumers groups found for stream: %s", pw.GetStreamName())
		return lastDeliveredId, lastDelivered, err
	}

	for _, result := range results {
		if result.Name == pw.GetGroupName() {
			lastDelivered = result.LastDeliveredID
			lastDeliveredId, err = splitId(lastDelivered)
			if err != nil {
				pw.setError("Error in getConsumerLag", err)
				return lastDeliveredId, lastDelivered, err
			}
		}
	}

	return lastDeliveredId, lastDelivered, nil

}

// getPending is used to get the list of the pending values
func (pw *RedisWriter) getPending(ctx context.Context) (*redis.XPendingCmd, error) {

	pending := pw.Client.XPending(ctx, pw.GetStreamName(), pw.GetGroupName())

	// see an error with pending, keep minId as 0-0
	if pending.Err() != nil {
		return nil, fmt.Errorf("error in getting XPending: %w", pending.Err())
	}
	return pending, nil
}

// IsFull returns whether the buffer is full. It could be approximate.
func (pw *RedisWriter) IsFull() bool {
	return pw.PartitionWriteInfo.isFull.Load()
}

// GetPendingCount is used to get the pendingCount value
func (pw *RedisWriter) GetPendingCount() int64 {
	return pw.PartitionWriteInfo.pendingCount.Load()
}

// GetBufferLength is used to get the partitionLength value
func (pw *RedisWriter) GetBufferLength() int64 {
	return pw.PartitionWriteInfo.partitionLength.Load()
}

// GetConsumerLag returns the consumerLag of the buffer
func (pw *RedisWriter) GetConsumerLag() time.Duration {
	return pw.PartitionWriteInfo.consumerLag.Load()
}

// GetMinId returns the MINID of the buffer
func (pw *RedisWriter) GetMinId() string {
	return pw.PartitionWriteInfo.minId.Load()
}

// HasUnprocessedData tells us if we have any unprocessed data left in the buffer
func (pw *RedisWriter) HasUnprocessedData() bool {
	return pw.PartitionWriteInfo.hasUnprocessedData.Load()
}

// GetRefreshFullError returns the refreshFullError count of the buffer
func (pw *RedisWriter) GetRefreshFullError() uint32 {
	return pw.PartitionWriteInfo.refreshFullError.Load()
}

// setIsFull is used to set the isFull value
func (pw *RedisWriter) setIsFull(flag bool) {
	pw.PartitionWriteInfo.isFull.Store(flag)
}

// setPendingCount is used to set the pendingCount value
func (pw *RedisWriter) setPendingCount(pendingCount int64) {
	pw.PartitionWriteInfo.pendingCount.Store(pendingCount)
}

// setBufferLength is used to set the partitionLength value
func (pw *RedisWriter) setBufferLength(bufferLength int64) {
	pw.PartitionWriteInfo.partitionLength.Store(bufferLength)
}

// setConsumerLag is used to set the consumerLag value
func (pw *RedisWriter) setConsumerLag(consumerLag time.Duration) {
	pw.PartitionWriteInfo.consumerLag.Store(consumerLag)
}

// setMinId is used to set the minId value
func (pw *RedisWriter) setMinId(minId string) {
	pw.PartitionWriteInfo.minId.Store(minId)
}

// setHasUnprocessed is used to set the value of hasProcessed.
func (pw *RedisWriter) setHasUnprocessed(hasUnprocessed bool) {
	pw.PartitionWriteInfo.hasUnprocessedData.Store(hasUnprocessed)
}

// setError is used to set error cases
func (pw *RedisWriter) setError(errMsg string, err error) {
	pw.log.Errorw(errMsg, zap.Error(err))
	pw.PartitionWriteInfo.refreshFullError.Inc()
}

// splitId is used to split the Id obtained from redis.
// An id is of the format "1434-0", where the first part is the timestamp.
// Today we care only about the timestamp and compare just that.
func splitId(id string) (int64, error) {
	splitId := strings.Split(id, "-")
	idValue, err := strconv.ParseInt(splitId[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ParseFloat err: %w", err)
	}
	return idValue, err

}
