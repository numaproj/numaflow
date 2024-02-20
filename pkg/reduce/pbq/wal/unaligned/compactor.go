package unaligned

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	CompactedPrefix = "compacted"
	CurrCompacted   = "current" + "-" + CompactedPrefix
)

// compactor is a compactor for the data files
type compactor struct {
	partitionID        *partition.ID
	compactKeyMap      map[string]int64
	storeEventsPath    string
	storeDataPath      string
	currCompactedFile  *os.File
	compWriteOffset    int64
	compBufWriter      *bufio.Writer
	prevSyncedTime     time.Time
	maxFileSize        int64
	mu                 sync.Mutex
	dc                 *decoder
	ec                 *encoder
	compactionDuration time.Duration
	syncDuration       time.Duration
	stopSignal         chan struct{}
	doneCh             chan struct{}
	log                *zap.SugaredLogger
}

// NewCompactor returns a new compactor instance
func NewCompactor(ctx context.Context, partitionId *partition.ID, storeEventsPath string, storeDataPath string, opts ...CompactorOption) (Compactor, error) {

	c := &compactor{
		compactionDuration: dfv1.DefaultCompactionDuration,
		maxFileSize:        dfv1.DefaultCompactorMaxFileSize,
		syncDuration:       dfv1.DefaultCompactorSyncDuration,
		partitionID:        partitionId,
		storeDataPath:      storeDataPath,
		storeEventsPath:    storeEventsPath,
		compactKeyMap:      make(map[string]int64),
		prevSyncedTime:     time.Now(),
		mu:                 sync.Mutex{},
		dc:                 newDecoder(),
		ec:                 newEncoder(),
		compWriteOffset:    0,
		doneCh:             make(chan struct{}),
		stopSignal:         make(chan struct{}),
		log:                logging.FromContext(ctx),
	}

	// Create wal dir if not exist
	var err error
	if _, err = os.Stat(c.storeDataPath); os.IsNotExist(err) {
		err = os.Mkdir(c.storeDataPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// Create event dir if not exist
	if _, err = os.Stat(c.storeEventsPath); os.IsNotExist(err) {
		err = os.Mkdir(c.storeEventsPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// open the first compaction file to write to
	if err = c.openCompactionFile(); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// Start starts the compactor
func (c *compactor) Start(ctx context.Context) error {
	// in case of incomplete compaction we should compact the data files
	// before starting the compactor

	// get all the events files
	eventFiles, err := filesInDir(c.storeEventsPath)
	if err != nil {
		return err
	}

	// there maybe some events which were not compacted
	// could happen if the compactor was stopped abruptly
	// so we should compact those before replaying persisted
	// messages
	if len(eventFiles) != 0 {
		if err = c.compact(ctx, eventFiles); err != nil {
			return err
		}
		// rotate the compaction file because we need to first replay the pending messages
		// if we don't rotate the file, the file name will have "current" in it and it
		// will not be considered for replay
		err = c.rotateCompactionFile()
		if err != nil {
			return err
		}
	}

	go c.keepCompacting(ctx)
	return nil
}

func (c *compactor) Stop() error {
	var err error

	//send the stop signal
	close(c.stopSignal)
	//wait for the compactor to stop
	<-c.doneCh
	// flush the buffer and sync the file
	if err = c.flushAndSync(); err != nil {
		return err
	}

	// Close the current data file
	if err = c.currCompactedFile.Close(); err != nil {
		return err
	}

	// delete the file if it's empty
	if c.compWriteOffset == 0 {
		return os.Remove(c.currCompactedFile.Name())
	}

	// rename the current compaction file to the segment file
	if err = os.Rename(c.currCompactedFile.Name(), c.getFilePath(c.storeDataPath)); err != nil {
		return err
	}

	return err
}

// getFilePath returns the file path for new file creation
func (c *compactor) getFilePath(storePath string) string {
	return filepath.Join(storePath, CompactedPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// keepCompacting keeps compacting the data files every compaction duration
func (c *compactor) keepCompacting(ctx context.Context) {
	compTimer := time.NewTicker(c.compactionDuration)
	for {
		select {
		case <-ctx.Done():
			close(c.doneCh)
			return
		case <-c.stopSignal:
			close(c.doneCh)
			return
		case <-compTimer.C:
			// get all the events files
			eventFiles, _ := filesInDir(c.storeEventsPath)
			err := c.compact(ctx, eventFiles)
			// TODO: retry, if its not ctx or stop signal error
			if err != nil {
				c.log.Errorw("Error while compacting", zap.Error(err))
			}
		}
	}

}

// compact reads all the events file and constructs the compaction key map
// and then compacts the data files based on the compaction key map
func (c *compactor) compact(ctx context.Context, eventFiles []os.FileInfo) error {
	if len(eventFiles) == 0 {
		return nil
	}
	startTime := time.Now()
	// build the compaction key map
	err := c.buildCompactionKeyMap(eventFiles)
	if err != nil {
		return err
	}

	// compact the data files based on the compaction key map
	err = c.compactDataFiles(ctx)
	if err != nil {
		return err
	}

	// delete the events files
	for _, eventFile := range eventFiles {
		if err = os.Remove(filepath.Join(c.storeEventsPath, eventFile.Name())); err != nil {
			return err
		}
	}

	c.log.Debugw("compaction completed", zap.Duration("duration", time.Since(startTime)))
	return nil
}

// buildCompactionKeyMap builds the compaction key map from the event files
func (c *compactor) buildCompactionKeyMap(eventFiles []os.FileInfo) error {
	c.compactKeyMap = make(map[string]int64)

	for _, eventFile := range eventFiles {
		// read the compact events file
		opFile, err := os.Open(filepath.Join(c.storeEventsPath, eventFile.Name()))
		if err != nil {
			return err
		}

		// iterate over all the delete events and delete the messages
		for {
			var cEvent *deletionMessage
			cEvent, _, err = c.dc.decodeDeletionMessage(opFile)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				// close the file before returning
				opFile.Close()
				return err
			}

			// track the max end time for each key
			if _, ok := c.compactKeyMap[cEvent.Key]; !ok {
				c.compactKeyMap[cEvent.Key] = cEvent.Et
			} else {
				if c.compactKeyMap[cEvent.Key] < cEvent.Et {
					c.compactKeyMap[cEvent.Key] = cEvent.Et
				}
			}
		}

		err = opFile.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// filesInDir lists all the files in the given directory except the current file
func filesInDir(dirPath string) ([]os.FileInfo, error) {

	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer func(dir *os.File) {
		_ = dir.Close()
	}(dir)

	// read all files from the dir
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	var cfs []os.FileInfo
	// ignore the files which has "current" in their file name
	// because it will in use by the writer
	for i := 0; i < len(files); i++ {
		if strings.Contains(files[i].Name(), "current") {
			continue
		}
		cfs = append(cfs, files[i])
	}

	// sort the files based on the mod time
	// so that order of compaction is maintained
	// when you have multiple segments to compact
	// we should compact the oldest segment first
	sort.Slice(cfs, func(i, j int) bool {
		return cfs[i].ModTime().Before(cfs[j].ModTime())
	})

	return cfs, nil
}

// compactDataFiles compacts the data files
func (c *compactor) compactDataFiles(ctx context.Context) error {

	// if there are no events to compact, return
	if len(c.compactKeyMap) == 0 {
		return nil
	}

	// get all the data files
	dataFiles, err := filesInDir(c.storeDataPath)
	if err != nil {
		return err
	}

	// iterate over all the data files and compact them
	for _, dataFile := range dataFiles {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopSignal:
			// send error, otherwise the compactor will delete
			// the event files
			return fmt.Errorf("compactor stopped")
		default:
			c.log.Debugw("compacting file", zap.String("file", dataFile.Name()))
			if err = c.compactFile(dataFile.Name()); err != nil {
				return err
			}
		}
	}

	return nil
}

// compactFile compacts the given data file
// it copies the messages from the data file to the compaction file if the message should not be deleted
// and deletes the data file after compaction
func (c *compactor) compactFile(fileName string) error {
	// open the data file (read only)
	dp, err := os.Open(filepath.Join(c.storeDataPath, fileName))
	if err != nil {
		return err
	}

	// close the file before returning
	defer func(dp *os.File) {
		_ = dp.Close()
	}(dp)

	// read and decode the unalignedWAL header
	_, err = c.dc.decodeHeader(dp)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err

	}

readLoop:
	for {
		// read and decode the unalignedWAL message header
		mp, err := decodeWALMessageHeader(dp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break readLoop // end of file reached, break the loop
			}
			return err
		}

		// read the key
		key := make([]byte, mp.KeyLen)
		err = binary.Read(dp, binary.LittleEndian, &key)
		if err != nil {
			return err
		}

		// read the payload
		var payload = make([]byte, mp.MessageLen)
		read, err := dp.Read(payload)
		if err != nil {
			return err
		}

		mp.Checksum = calculateChecksum(payload)

		if read != int(mp.MessageLen) {
			return fmt.Errorf("expected to read length of %d, but read only %d", mp.MessageLen, read)
		}

		// skip deleted messages
		// we should copy the message only if the message should not be deleted
		if !c.shouldDeleteMessage(mp.EventTime, string(key)) {
			continue
		}

		// write the message to the output file
		if err = c.writeToFile(mp, string(key), payload); err != nil {
			return err
		}

	}

	// delete the data file, since it's been compacted
	if err = os.Remove(filepath.Join(c.storeDataPath, fileName)); err != nil {
		return err
	}
	return nil
}

// shouldDeleteMessage checks if the message should be deleted or not
func (c *compactor) shouldDeleteMessage(eventTime int64, key string) bool {
	// check if the key is present in the compaction key map
	ce, ok := c.compactKeyMap[key]

	// we should only delete the messages which are older than the max end time
	if ok && ce < eventTime {
		return true
	}
	return false
}

// writeToFile writes the message to the compacted file and rotates the file if the max file size is reached
func (c *compactor) writeToFile(header *readMessageHeaderPreamble, key string, payload []byte) error {
	buf := new(bytes.Buffer)
	// write the header
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return err
	}

	// write the key
	if err := binary.Write(buf, binary.LittleEndian, []byte(key)); err != nil {
		return err
	}

	// write the payload
	if wrote, err := buf.Write(payload); err != nil {
		return err
	} else if wrote != len(payload) {
		return fmt.Errorf("expected to write %d, but wrote only %d", len(payload), wrote)
	}

	bytesCount, err := c.compBufWriter.Write(buf.Bytes())
	if err != nil {
		return err
	}

	// update the write offset
	c.compWriteOffset += int64(bytesCount)

	// sync the file if the sync duration is reached
	if time.Since(c.prevSyncedTime) > c.syncDuration {
		if err = c.flushAndSync(); err != nil {
			return err
		}
	}

	if c.compWriteOffset >= c.maxFileSize {
		if err = c.rotateCompactionFile(); err != nil {
			return err
		}
	}

	return nil
}

// rotateCompactionFile rotates the compaction file
func (c *compactor) rotateCompactionFile() error {

	// flush the buffer and sync the file
	if err := c.flushAndSync(); err != nil {
		return err
	}

	// Close the current data file
	if err := c.currCompactedFile.Close(); err != nil {
		return err
	}

	// rename the current compaction file to the segment file
	if err := os.Rename(c.currCompactedFile.Name(), c.getFilePath(c.storeDataPath)); err != nil {
		return err
	}

	// Open the next data file
	return c.openCompactionFile()
}

// flushAndSync flushes and syncs the compaction file
func (c *compactor) flushAndSync() error {
	if err := c.compBufWriter.Flush(); err != nil {
		return err
	}
	if err := c.currCompactedFile.Sync(); err != nil {
		return err
	}

	// update the previous synced time
	c.prevSyncedTime = time.Now()
	return nil
}

// openCompactionFile opens a new compaction file
func (c *compactor) openCompactionFile() error {
	var err error
	c.currCompactedFile, err = os.OpenFile(filepath.Join(c.storeDataPath, CurrCompacted), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	// reset the offset
	c.compWriteOffset = 0

	// reset the write buffer
	if c.compBufWriter == nil {
		c.compBufWriter = bufio.NewWriter(c.currCompactedFile)
	} else {
		c.compBufWriter.Reset(c.currCompactedFile)
	}
	return c.writeWALHeader()
}

// decodeWALMessageHeader decodes the WALMessage header which is encoded by encodeWALMessageHeader.
func decodeWALMessageHeader(buf io.Reader) (*readMessageHeaderPreamble, error) {
	var entryHeader = new(readMessageHeaderPreamble)
	err := binary.Read(buf, binary.LittleEndian, entryHeader)
	if err != nil {
		return nil, err
	}
	return entryHeader, nil
}

// writeWALHeader writes the unalignedWAL header to the file.
func (c *compactor) writeWALHeader() error {
	header, err := c.ec.encodeHeader(c.partitionID)
	if err != nil {
		return err
	}
	wrote, err := c.compBufWriter.Write(header)
	if wrote != len(header) {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", len(header), wrote, err)
	}

	return err
}
