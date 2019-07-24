package service

import "context"

// Atomix protocol client
type Client interface {
	// Write sends a write request
	Write(ctx context.Context, input []byte, ch chan<- Output) error

	// Read sends a read request
	Read(ctx context.Context, input []byte, ch chan<- Output) error
}
