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

import (
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/driver/env"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
)

// NewProtocol creates a new state machine protocol
func NewProtocol(cluster cluster.Cluster, env env.DriverEnv) *Protocol {
	member, _ := cluster.Member()
	log := logging.GetLogger("atomix", "proxy", string(member.ID))
	return &Protocol{
		Server:     server.NewServer(cluster),
		Client:     NewClient(cluster, log),
		Env:        env,
		primitives: primitive.NewPrimitiveTypeRegistry(),
		log:        log,
	}
}

// Protocol is a state machine protocol
type Protocol struct {
	*server.Server
	Client     *Client
	Env        env.DriverEnv
	primitives *primitive.PrimitiveTypeRegistry
	log        logging.Logger
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
	n.log.Info("Starting protocol")
	n.Services().RegisterService(func(s *grpc.Server) {
		for _, primitiveType := range n.Primitives().ListPrimitiveTypes() {
			primitiveType.RegisterServer(s)
		}
	})
	n.Services().RegisterService(func(s *grpc.Server) {
		RegisterPrimitiveServer(s, n.Client, n.Env)
	})
	if err := n.Server.Start(); err != nil {
		n.log.Error(err, "Starting protocol")
		return err
	}
	if err := n.Client.Connect(); err != nil {
		n.log.Error(err, "Starting protocol")
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
	if err := n.Client.Close(); err != nil {
		return err
	}
	return nil
}
