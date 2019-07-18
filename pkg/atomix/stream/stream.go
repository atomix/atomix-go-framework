package stream

// Stream is an interface for state machine response streams
type Stream interface {

	// Next pushes the next response value onto the stream
	Next(value []byte)

	// Fail closes the stream with the given error
	Fail(err error)

	// Complete closes the stream cleanly
	Complete()
}
