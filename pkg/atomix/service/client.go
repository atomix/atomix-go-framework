package service

// Atomix protocol client
type Client interface {
	// Write sends a write request
	Write(input []byte, ch chan<- *Result) error

	// Read sends a read request
	Read(input []byte, ch chan<- *Result) error
}
