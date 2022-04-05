// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"context"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/primitive"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "proxy", "rsm")

// NewProtocol creates a new state machine protocol
func NewProtocol(cluster cluster.Cluster, env env.DriverEnv) *Protocol {
	return &Protocol{
		Server:     server.NewServer(cluster),
		Client:     NewClient(cluster, env),
		Env:        env,
		primitives: primitive.NewPrimitiveTypeRegistry(),
	}
}

// Protocol is a state machine protocol
type Protocol struct {
	*server.Server
	Client     *Client
	Env        env.DriverEnv
	primitives *primitive.PrimitiveTypeRegistry
}

// Name returns the protocol name
func (n *Protocol) Name() string {
	member, _ := n.Cluster.Member()
	return string(member.ID)
}

// Primitives returns the protocol primitives
func (n *Protocol) Primitives() *primitive.PrimitiveTypeRegistry {
	return n.primitives
}

// Start starts the node
func (n *Protocol) Start() error {
	log.Info("Starting protocol")
	n.Services().RegisterService(func(s *grpc.Server) {
		for _, primitiveType := range n.Primitives().ListPrimitiveTypes() {
			primitiveType.RegisterServer(s)
		}
	})
	n.Services().RegisterService(func(s *grpc.Server) {
		RegisterPrimitiveServer(s, n.Client, n.Env)
	})
	if err := n.Server.Start(); err != nil {
		log.Error(err, "Starting protocol")
		return err
	}
	if err := n.Client.Connect(context.Background()); err != nil {
		log.Error(err, "Starting protocol")
		return err
	}
	return nil
}

// Configure configures the protocol
func (n *Protocol) Configure(config protocolapi.ProtocolConfig) error {
	if configurable, ok := n.Cluster.(cluster.ConfigurableCluster); ok {
		return configurable.Update(config)
	}
	return nil
}

// Stop stops the node
func (n *Protocol) Stop() error {
	if err := n.Server.Stop(); err != nil {
		return err
	}
	if err := n.Client.Close(context.Background()); err != nil {
		return err
	}
	return nil
}
