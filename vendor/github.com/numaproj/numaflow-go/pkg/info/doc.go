// Package info is used for the gRPC server to provide the information such as protocol, sdk version, language, etc, to the client.
//
// The server information can be used by the client to determine:
//   - what is the right protocol to use (UDS or TCP)
//   - what is the numaflow sdk version used by the server
//   - what is language used by the server
//
// The gRPC server (UDF, UDSink, etc.) is supposed to have a shared file system with the client (numa container).
//
// Write()
// The gPRC server must use this function to write the correct ServerInfo when it starts.
//
// Read()
// The client is supposed to call the function to read the server information, before it starts to communicate with the gRPC server.
//
// WaitUntilReady()
// This function checks if the server info file is ready to read.
// The client (numa container) is supposed to call the function before it starts to Read() the server info file.
package info
