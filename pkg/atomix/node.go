package atomix

import "github.com/atomix/atomix-go-node/pkg/atomix/stream"

// Atomix protocol client
type Client interface {

	// Write sends a synchronous write request
	Write([]byte) ([]byte, error)

	// WriteStream sends a write request and awaits a streaming response
	WriteStream([]byte, stream.Stream) error

	// Read sends a synchronous read request
	Read([]byte) ([]byte, error)

	// ReadStream sends a read request and awaits a streaming response
	ReadStream([]byte, stream.Stream) error

}

// Atomix node
type Node interface {

	// Start starts the Atomix node
	Start() error

	// Stop stops the Atomix node
	Stop() error

}
