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
	"strings"
	"sync"
	"time"
)

const (
	CompactedPrefix = "compacted"
)

type compactor struct {
	compactKeyMap      map[string]int64
	storeEventsPath    string
	storeDataPath      string
	currCompactedFile  *os.File
	compWriteOffset    int64
	compBufWriter      *bufio.Writer
	prevSyncedTime     time.Time
	maxFileSize        int64
	mu                 sync.Mutex
	decoder            *Decoder
	compactionDuration time.Duration
	syncDuration       time.Duration
}

// NewCompactor returns a new compactor instance
func NewCompactor(storeDataPath string, storeEventsPath string, opts ...CompactorOption) (Compactor, error) {

	c := &compactor{
		storeDataPath:   storeDataPath,
		storeEventsPath: storeEventsPath,
		compactKeyMap:   make(map[string]int64),
		prevSyncedTime:  time.Now(),
		mu:              sync.Mutex{},
		decoder:         NewDecoder(),
		compWriteOffset: 0,
	}

	if err := c.openCompactionFile(); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(c)
	}

	c.compBufWriter = bufio.NewWriter(c.currCompactedFile)

	return c, nil
}

// Start starts the compactor
func (c *compactor) Start(ctx context.Context) error {
	// TODO write logic to compact if there are any event files left
	go c.keepCompacting(ctx)
	return nil
}

func (c *compactor) Stop(ctx context.Context) error {
	// TODO
	return nil
}

// getFilePath returns the file path for new file creation
func (c *compactor) getFilePath(storePath string) string {
	return filepath.Join(storePath, CompactedPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// keepCompacting keeps compacting the data files every compaction duration
func (c *compactor) keepCompacting(ctx context.Context) {
	compTimer := time.NewTimer(c.compactionDuration)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			<-compTimer.C
		}
		// TODO error handling
		eventFiles, _ := filesInDir(c.storeEventsPath)
		// build the compaction key map
		c.buildCompactionKeyMap(eventFiles)
		// compact the data files based on the compaction key map
		c.compactDataFiles(ctx)
	}

}

func (c *compactor) buildCompactionKeyMap(eventFiles []os.FileInfo) {
	c.compactKeyMap = make(map[string]int64)

	for _, eventFile := range eventFiles {
		// read the compact events file
		opFile, err := os.Open(filepath.Join(c.storeEventsPath, eventFile.Name()))
		if err != nil {
			continue
		}

		// iterate over all the delete events and delete the messages
		for {
			cEvent, _, err := c.decoder.DecodeDeleteMessage(opFile)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				continue
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
	}
}

// filesInDir lists all the files in the given directory except the current file
func filesInDir(dirPath string) ([]os.FileInfo, error) {

	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	// read all files from the dir
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	if len(files) <= 1 {
		return []os.FileInfo{}, nil
	}

	// ignore the files which has "current" in their file name
	for i := 0; i < len(files); i++ {
		if strings.Contains(files[i].Name(), "current") {
			files = append(files[:i], files[i+1:]...)
			break
		}
	}

	// return the files expect the last one because its being
	// used as the current data file
	return files, nil
}

func (c *compactor) compactDataFiles(ctx context.Context) error {

	dataFiles, err := filesInDir(c.storeDataPath)
	if err != nil {
		return err
	}

	// iterate over all the data files and compact them
	for _, dataFile := range dataFiles {
		if err := c.compactFile(dataFile.Name()); err != nil {
			return err
		}
	}

	return nil
}

func (c *compactor) compactFile(fileName string) error {
	dp, err := os.Open(filepath.Join(c.storeDataPath, fileName))
	if err != nil {
		return err
	}

	for {
		// read and decode the WAL header
		_, err := c.decoder.DecodeHeader(dp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // end of file reached, break the loop
			} else {
				return err
			}
		}

		// read and decode the WAL message header
		mp, err := decodeWALMessageHeader(dp)
		if err != nil {
			return err
		}

		// read the variadic slot
		key := make([]rune, mp.KeyLen)
		err = binary.Read(dp, binary.LittleEndian, &key)
		if err != nil {
			return err
		}

		// skip deleted messages
		// we should copy the message only if the message should not be deleted
		if !c.shouldDeleteMessage(mp.EventTime, string(key)) {
			continue
		}

		// read the payload
		var payload = make([]byte, mp.MessageLen)
		err = binary.Read(dp, binary.LittleEndian, payload)
		if err != nil {
			return err
		}

		// write the message to the output file
		if err := c.writeToFile(mp, string(key), payload); err != nil {
			return err
		}

	}

	// delete the data file, since its been compactecd
	if err := os.Remove(filepath.Join(c.storeDataPath, fileName)); err != nil {
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
	// write the message to the output file
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return err
	}

	// write the key
	if err := binary.Write(buf, binary.LittleEndian, []byte(key)); err != nil {
		return err
	}

	// write the payload
	if err := binary.Write(buf, binary.LittleEndian, payload); err != nil {
		return err
	}

	bytesCount, err := c.compBufWriter.Write(buf.Bytes())
	if err != nil {
		return err
	}

	// sync the file if the sync duration is reached
	if time.Since(c.prevSyncedTime) > c.syncDuration {
		if err := c.flushAndSync(); err != nil {
			return err
		}
		c.prevSyncedTime = time.Now()
	}

	c.compWriteOffset += int64(bytesCount)

	if c.compWriteOffset >= c.maxFileSize {
		_ = c.rotateCompactionFile()
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

	// Open the next data file
	return c.openCompactionFile()
}

// flushAndSync flushes and syncs the compaction file
func (c *compactor) flushAndSync() error {
	if err := c.compBufWriter.Flush(); err != nil {
		return err
	}
	return c.currCompactedFile.Sync()
}

// openCompactionFile opens a new compaction file
func (c *compactor) openCompactionFile() error {
	var err error
	c.currCompactedFile, err = os.OpenFile(c.getFilePath(c.storeDataPath), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return nil
}

// decodeWALMessageHeader decodes the WALMessage header which is encoded by encodeWALMessageHeader.
func decodeWALMessageHeader(buf io.Reader) (*readMessageHeaderPreamble, error) {
	// read the fixed vals
	var entryHeader = new(readMessageHeaderPreamble)
	err := binary.Read(buf, binary.LittleEndian, entryHeader)
	if err != nil {
		return nil, err
	}
	return entryHeader, nil
}
