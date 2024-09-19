package httpexpect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
)

// Wrapper for request or response body reader.
//
// Allows to read body multiple times using two approaches:
//   - use Read to read body contents and Rewind to restart reading from beginning
//   - use GetBody to get new reader for body contents
//
// When bodyWrapper is created, it does not read anything. Also, until anything is
// read, rewind operations are no-op.
//
// When the user starts reading body, bodyWrapper automatically copies retrieved
// content in memory. Then, when the body is fully read and Rewind is requested,
// it will close original body and switch to reading body from memory.
//
// If Rewind, GetBody, or Close is invoked before the body is fully read first time,
// bodyWrapper automatically performs full read.
//
// At any moment, the user can call DisableRewinds. In this case, Rewind and GetBody
// functionality is disabled, memory cache is cleared, and bodyWrapper switches to
// reading original body (if it's not fully read yet).
//
// bodyWrapper automatically creates finalizer that will close original body if the
// user never reads it fully or calls Closes.
type bodyWrapper struct {
	// Protects all operations.
	mu sync.Mutex

	// Original reader of HTTP response body.
	httpReader io.ReadCloser

	// Cancellation function for original HTTP response.
	// If set, called after HTTP response is fully read into memory.
	httpCancelFunc context.CancelFunc

	// Reader for HTTP response body stored in memory.
	// Rewind() resets this reader to start from the beginning.
	memReader *bytes.Reader

	// HTTP response body stored in memory.
	memBytes []byte

	// Cached read and close errors.
	readErr  error
	closeErr error

	// If true, Read will not store bytes in memory, and memBytes and memReader
	// won't be used.
	isRewindDisabled bool

	// True means that HTTP response was fully read into memory already.
	isFullyRead bool

	// True means that a read operation of any type was called at least once.
	isReadBefore bool
}

func newBodyWrapper(reader io.ReadCloser, cancelFunc context.CancelFunc) *bodyWrapper {
	bw := &bodyWrapper{
		httpReader:     reader,
		httpCancelFunc: cancelFunc,
	}

	// Finalizer will close body if closeAndCancel was never called.
	runtime.SetFinalizer(bw, (*bodyWrapper).Close)

	return bw
}

// Read body contents.
func (bw *bodyWrapper) Read(p []byte) (int, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.isReadBefore = true

	if bw.isRewindDisabled && !bw.isFullyRead {
		// Regular read from original HTTP response.
		return bw.httpReader.Read(p)
	} else if !bw.isFullyRead {
		// Read from original HTTP response + store into memory.
		return bw.httpReadNext(p)
	} else {
		// Read from memory.
		return bw.memReadNext(p)
	}
}

// Close body.
func (bw *bodyWrapper) Close() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Preserve original reader error.
	err := bw.closeErr

	// Rewind or GetBody may be called later, so be sure to
	// read body into memory before closing.
	if !bw.isRewindDisabled && !bw.isFullyRead {
		bw.isReadBefore = true

		if readErr := bw.httpReadFull(); readErr != nil {
			err = readErr
		}
	}

	// Close original reader.
	closeErr := bw.closeAndCancel()
	if closeErr != nil {
		err = closeErr
	}

	// Reset memory reader.
	bw.memReader = bytes.NewReader(nil)

	// Free memory when rewind is disabled.
	if bw.isRewindDisabled {
		bw.memBytes = nil
	}

	return err
}

// Rewind reading to the beginning.
func (bw *bodyWrapper) Rewind() {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Rewind is no-op if disabled.
	if bw.isRewindDisabled {
		return
	}

	// Rewind is no-op until first read operation.
	if !bw.isReadBefore {
		return
	}

	// If HTTP response is not fully read yet, do it now.
	// If error occurs, it will be reported next read operation.
	if !bw.isFullyRead {
		_ = bw.httpReadFull()
	}

	// Reset memory reader.
	bw.memReader = bytes.NewReader(bw.memBytes)
}

// Create new reader to retrieve body contents.
// New reader always reads body from the beginning.
// Does not affected by Rewind().
func (bw *bodyWrapper) GetBody() (io.ReadCloser, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.isReadBefore = true

	// Preserve original reader error.
	if bw.readErr != nil {
		return nil, bw.readErr
	}

	// GetBody() requires rewinds to be enabled.
	if bw.isRewindDisabled {
		return nil, errors.New("rewinds are disabled, cannot get body")
	}

	// If HTTP response is not fully read yet, do it now.
	if !bw.isFullyRead {
		if err := bw.httpReadFull(); err != nil {
			return nil, err
		}
	}

	// Return fresh reader for memory chunk.
	return io.NopCloser(bytes.NewReader(bw.memBytes)), nil
}

// Disables storing body contents in memory and clears the cache.
func (bw *bodyWrapper) DisableRewinds() {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Free memory if reading from original HTTP response, or reading from memory
	// and memory reader has nothing left to read.
	// Otherwise, i.e. when we're reading from memory, and there is more to read,
	// memReadNext() will free memory later when it hits EOF.
	if !bw.isFullyRead || bw.memReader.Len() == 0 {
		bw.memReader = bytes.NewReader(nil)
		bw.memBytes = nil
	}

	bw.isRewindDisabled = true
}

func (bw *bodyWrapper) memReadNext(p []byte) (int, error) {
	n, err := bw.memReader.Read(p)

	if err == io.EOF {
		// Free memory after we hit EOF when reading from memory,
		// if rewinds were disabled while we were reading from it.
		if bw.isRewindDisabled {
			bw.memReader = bytes.NewReader(nil)
			bw.memBytes = nil
		}
		if bw.readErr != nil {
			err = bw.readErr
		}
	}

	return n, err
}

func (bw *bodyWrapper) httpReadNext(p []byte) (int, error) {
	n, err := bw.httpReader.Read(p)

	if n > 0 {
		bw.memBytes = append(bw.memBytes, p[:n]...)
	}

	if err != nil {
		if err != io.EOF {
			bw.readErr = err
		}
		if closeErr := bw.closeAndCancel(); closeErr != nil && err == io.EOF {
			err = closeErr
		}

		// Switch to reading from memory.
		bw.isFullyRead = true
		bw.memReader = bytes.NewReader(nil)
	}

	return n, err
}

func (bw *bodyWrapper) httpReadFull() error {
	b, err := io.ReadAll(bw.httpReader)

	// Switch to reading from memory.
	bw.isFullyRead = true
	bw.memBytes = append(bw.memBytes, b...)
	bw.memReader = bytes.NewReader(bw.memBytes[len(bw.memBytes)-len(b):])

	if err != nil {
		bw.readErr = err
	}

	if closeErr := bw.closeAndCancel(); closeErr != nil && err == nil {
		err = closeErr
	}

	return err
}

func (bw *bodyWrapper) closeAndCancel() error {
	if bw.httpReader == nil && bw.httpCancelFunc == nil {
		return bw.closeErr
	}

	if bw.httpReader != nil {
		err := bw.httpReader.Close()
		bw.httpReader = nil

		if bw.readErr == nil {
			bw.readErr = err
		}

		if bw.closeErr == nil {
			bw.closeErr = err
		}
	}

	if bw.httpCancelFunc != nil {
		bw.httpCancelFunc()
		bw.httpCancelFunc = nil
	}

	// Finalizer is not needed anymore.
	runtime.SetFinalizer(bw, nil)

	return bw.closeErr
}
