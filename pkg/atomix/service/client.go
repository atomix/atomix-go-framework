package service

// Atomix protocol client
type Client interface {
	// Write sends a synchronous write request
	Write([]byte) ([]byte, error)

	// WriteStream sends a write request and awaits a streaming response
	WriteStream([]byte, Stream) error

	// Read sends a synchronous read request
	Read([]byte) ([]byte, error)

	// ReadStream sends a read request and awaits a streaming response
	ReadStream([]byte, Stream) error
}
