package service

// Atomix protocol client
type Client interface {
	// Write sends a write request
	Write(input []byte, ch chan<- Output) error

	// Read sends a read request
	Read(input []byte, ch chan<- Output) error
}
