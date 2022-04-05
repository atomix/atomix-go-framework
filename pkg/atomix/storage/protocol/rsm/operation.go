// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
