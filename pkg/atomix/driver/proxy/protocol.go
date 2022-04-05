// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/server"
)

// ProtocolFunc is a protocol factory function
type ProtocolFunc func(cluster cluster.Cluster, env env.DriverEnv) Protocol

// Protocol is a proxy protocol
type Protocol interface {
	server.Node

	// Name returns the protocol name
	Name() string

	// Primitives returns the protocol primitives
	Primitives() *primitive.PrimitiveTypeRegistry

	// Configure configures the protocol
	Configure(config protocolapi.ProtocolConfig) error
}
