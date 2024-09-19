package info

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

var END = fmt.Sprintf("%U__END__", '\\') // U+005C__END__

func getSDKVersion() string {
	version := ""
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	for _, d := range info.Deps {
		if strings.HasSuffix(d.Path, "/numaflow-go") {
			version = d.Version
			break
		}
	}
	return version
}

// Write writes the server info to a file
func Write(svrInfo *ServerInfo, opts ...Option) error {
	b, err := json.Marshal(svrInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal server info: %w", err)
	}
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	if err := os.Remove(options.svrInfoFilePath); !os.IsNotExist(err) && err != nil {
		return fmt.Errorf("failed to remove server-info file: %w", err)
	}
	f, err := os.Create(options.svrInfoFilePath)
	if err != nil {
		return fmt.Errorf("failed to create server-info file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(b)
	if err != nil {
		return fmt.Errorf("failed to write server-info file: %w", err)
	}
	_, err = f.WriteString(END)
	if err != nil {
		return fmt.Errorf("failed to write END server-info file: %w", err)
	}
	return nil
}

// WaitUntilReady waits until the server info is ready
func WaitUntilReady(ctx context.Context, opts ...Option) error {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if fileInfo, err := os.Stat(options.svrInfoFilePath); err != nil {
				log.Printf("Server info file %s is not ready...", options.svrInfoFilePath)
				time.Sleep(1 * time.Second)
				continue
			} else {
				if fileInfo.Size() > 0 {
					return nil
				}
			}
		}
	}
}

// Read reads the server info from a file
func Read(opts ...Option) (*ServerInfo, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	// It takes some time for the server to write the server info file
	// TODO: use a better way to wait for the file to be ready
	retry := 0
	b, err := os.ReadFile(options.svrInfoFilePath)
	for !strings.HasSuffix(string(b), END) && err == nil && retry < 10 {
		time.Sleep(100 * time.Millisecond)
		b, err = os.ReadFile(options.svrInfoFilePath)
		retry++
	}
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(string(b), END) {
		return nil, fmt.Errorf("server info file is not ready")
	}
	b = b[:len(b)-len([]byte(END))]
	info := &ServerInfo{}
	if err := json.Unmarshal(b, info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server info: %w", err)
	}
	return info, nil
}

// GetDefaultServerInfo returns a ServerInfo object with the default fields populated for Go-SDK
func GetDefaultServerInfo() *ServerInfo {
	serverInfo := &ServerInfo{Protocol: UDS, Language: Go, MinimumNumaflowVersion: MinimumNumaflowVersion,
		Version: getSDKVersion()}
	return serverInfo

}
