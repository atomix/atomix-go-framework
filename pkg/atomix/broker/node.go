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

package broker

import (
	coordinatorapi "github.com/atomix/api/go/atomix/management/broker"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	protocolapi "github.com/atomix/api/go/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "broker")

const (
	controllerPort = 5151
	primitivePort  = 5678
)

// NewNode creates a new broker node
func NewNode() *Node {
	return &Node{
		controllerServer: server.NewServer(cluster.NewCluster(
			protocolapi.ProtocolConfig{},
			cluster.WithMemberID("broker"),
			cluster.WithPort(controllerPort))),
		primitiveServer:  server.NewServer(cluster.NewCluster(
			protocolapi.ProtocolConfig{},
			cluster.WithMemberID("broker"),
			cluster.WithPort(primitivePort))),
	}
}

// Node is a broker node
type Node struct {
	controllerServer *server.Server
	primitiveServer  *server.Server
}

// Start starts the node
func (n *Node) Start() error {
	server := NewServer(newDriverRegistry(), newPrimitiveRegistry())
	n.controllerServer.RegisterService(func(s *grpc.Server) {
		coordinatorapi.RegisterPrimitiveManagementServiceServer(s, server)
		coordinatorapi.RegisterDriverManagementServiceServer(s, server)
	})
	n.primitiveServer.RegisterService(func(s *grpc.Server) {
		primitiveapi.RegisterPrimitiveDiscoveryServiceServer(s, server)
	})
	if err := n.controllerServer.Start(); err != nil {
		return err
	}
	if err := n.primitiveServer.Start(); err != nil {
		return err
	}
	return nil
}

// Stop stops the node
func (n *Node) Stop() error {
	if err := n.controllerServer.Stop(); err != nil {
		return err
	}
	if err := n.primitiveServer.Stop(); err != nil {
		return err
	}
	return nil
}
