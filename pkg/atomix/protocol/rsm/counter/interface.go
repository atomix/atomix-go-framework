package counter

import (
	counter "github.com/atomix/api/go/atomix/primitive/counter"
)

type Service interface {
	// Set sets the counter value
	Set(*counter.SetInput) (*counter.SetOutput, error)
	// Get gets the current counter value
	Get(*counter.GetInput) (*counter.GetOutput, error)
	// Increment increments the counter value
	Increment(*counter.IncrementInput) (*counter.IncrementOutput, error)
	// Decrement decrements the counter value
	Decrement(*counter.DecrementInput) (*counter.DecrementOutput, error)
	// Snapshot exports a snapshot of the primitive state
	Snapshot() (*counter.Snapshot, error)
	// Restore imports a snapshot of the primitive state
	Restore(*counter.Snapshot) error
}
