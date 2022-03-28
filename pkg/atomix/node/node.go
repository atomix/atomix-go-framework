// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package node

// Node is the base interface for all nodes
type Node interface {
	// Start starts the node
	Start() error
	// Stop stops the node
	Stop() error
}
