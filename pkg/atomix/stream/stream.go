// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stream

// Stream is a state machine output stream
type Stream interface {
	// Send sends an output on the stream
	Send(out Result)

	// Result sends a result on the stream
	Result(value []byte, err error)

	// Value sends a value on the stream
	Value(value []byte)

	// Error sends an error on the stream
	Error(err error)

	// Close closes the stream
	Close()
}

// NewChannelStream returns a new channel-based stream
func NewChannelStream(ch chan<- Result) Stream {
	return &channelStream{
		ch: ch,
	}
}

// channelStream is a channel-based stream
type channelStream struct {
	ch chan<- Result
}

func (s *channelStream) Send(result Result) {
	s.ch <- result
}

func (s *channelStream) Result(value []byte, err error) {
	s.Send(Result{
		Value: value,
		Error: err,
	})
}

func (s *channelStream) Value(value []byte) {
	s.Result(value, nil)
}

func (s *channelStream) Error(err error) {
	s.Result(nil, err)
}

func (s *channelStream) Close() {
	close(s.ch)
}

// NewNilStream returns a disconnected stream
func NewNilStream() Stream {
	return &nilStream{}
}

// nilStream is a stream that does not send messages
type nilStream struct{}

func (s *nilStream) Send(out Result) {
}

func (s *nilStream) Result(value []byte, err error) {
}

func (s *nilStream) Value(value []byte) {
}

func (s *nilStream) Error(err error) {
}

func (s *nilStream) Close() {
}

// NewEncodingStream returns a new encoding stream
func NewEncodingStream(stream Stream, encoder func([]byte) ([]byte, error)) Stream {
	return &encodingStream{
		stream:  stream,
		encoder: encoder,
	}
}

// encodingStream is a stream that encodes output
type encodingStream struct {
	stream  Stream
	encoder func([]byte) ([]byte, error)
}

func (s *encodingStream) Send(result Result) {
	if result.Failed() {
		s.stream.Send(result)
	} else {
		s.Value(result.Value)
	}
}

func (s *encodingStream) Result(value []byte, err error) {
	if err != nil {
		s.stream.Error(err)
	} else {
		s.Value(value)
	}
}

func (s *encodingStream) Value(value []byte) {
	bytes, err := s.encoder(value)
	if err != nil {
		s.stream.Error(err)
	} else {
		s.stream.Value(bytes)
	}
}

func (s *encodingStream) Error(err error) {
	s.stream.Error(err)
}

func (s *encodingStream) Close() {
	s.stream.Close()
}

// Result is a stream result
type Result struct {
	Value []byte
	Error error
}

// Failed returns a boolean indicating whether the operation failed
func (r Result) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Result) Succeeded() bool {
	return !r.Failed()
}
