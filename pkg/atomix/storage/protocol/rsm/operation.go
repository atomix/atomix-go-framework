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

package rsm

// OperationID is an operation identifier
type OperationID uint32

// Operation is a command or query operation
type Operation interface {
	// OperationID returns the operation identifier
	OperationID() OperationID
	// Session returns the session executing the operation
	Session() Session
	// Input returns the operation input
	Input() []byte
	// Output returns the operation output
	Output([]byte, error)
	// Close closes the operation
	Close()
}

func newOperation(session Session) *primitiveOperation {
	return &primitiveOperation{
		session: session,
	}
}

type primitiveOperation struct {
	session Session
}

func (o *primitiveOperation) Session() Session {
	return o.session
}
