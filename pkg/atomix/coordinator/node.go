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

package coordinator

import (
	driverapi "github.com/atomix/api/go/atomix/driver"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/drivers"
	"github.com/atomix/go-framework/pkg/atomix/primitives"
	"github.com/atomix/go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
	"sync"
)

// NewNode creates a new coordinator node
func NewNode(cluster *cluster.Cluster) *Node {
	return &Node{
		node:    server.NewNode(cluster),
		drivers: drivers.NewRegistry(),
	}
}

// Node is a coordinator node
type Node struct {
	node    *server.Node
	drivers *drivers.Registry
	mu      sync.RWMutex
}

// RegisterDriver registers a driver proxy
func (n *Node) RegisterDriver(driver driverapi.DriverMeta) error {
	return n.drivers.RegisterDriver(driver)
}

// Start starts the node
func (n *Node) Start() error {
	n.node.RegisterService(func(s *grpc.Server) {
		server := drivers.NewServer(n.drivers)
		driverapi.RegisterDriverServiceServer(s, server)
	})
	n.node.RegisterService(func(s *grpc.Server) {
		server := primitives.NewServer(n.drivers, primitives.NewRegistry())
		primitiveapi.RegisterPrimitiveRegistryServiceServer(s, server)
		primitiveapi.RegisterPrimitiveManagementServiceServer(s, server)
	})
	return n.node.Start()
}

// Stop stops the node
func (n *Node) Stop() error {
	return n.node.Stop()
}
