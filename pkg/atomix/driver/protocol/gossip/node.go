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

package gossip

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/driver/primitive"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	proxy "github.com/atomix/go-framework/pkg/atomix/proxy/gossip"
	"github.com/atomix/go-framework/pkg/atomix/server"
	"github.com/atomix/go-framework/pkg/atomix/time"
	"google.golang.org/grpc"
	"strings"
)

// NewNodeFromEnv creates a new server node from environment variables
func NewNode(cluster cluster.Cluster, scheme time.Scheme) *Node {
	member, _ := cluster.Member()
	return &Node{
		Server:     server.NewServer(cluster),
		Client:     proxy.NewClient(cluster, scheme),
		primitives: primitive.NewPrimitiveTypeRegistry(),
		log:        logging.GetLogger("atomix", "driver", strings.ToLower(string(member.ID))),
	}
}

// Node is an Atomix node
type Node struct {
	*server.Server
	Client     *proxy.Client
	primitives *primitive.PrimitiveTypeRegistry
	log        logging.Logger
}

// RegisterPrimitiveType registers a primitive type
func (n *Node) RegisterPrimitiveType(primitiveType primitive.PrimitiveType) {
	n.primitives.RegisterPrimitiveType(primitiveType)
}

// Start starts the node
func (n *Node) Start() error {
	n.Services().RegisterService(func(s *grpc.Server) {
		for _, primitiveType := range n.primitives.ListPrimitiveTypes() {
			primitiveType.RegisterServer(s)
		}
	})
	n.Services().RegisterService(func(s *grpc.Server) {
		server := newServer(n)
		driverapi.RegisterProxyManagementServiceServer(s, server)
		driverapi.RegisterDriverManagementServiceServer(s, server)
		proxy.RegisterPrimitiveServer(s, n.Client)
	})
	if err := n.Server.Start(); err != nil {
		return err
	}
	return nil
}

// Stop stops the node
func (n *Node) Stop() error {
	if err := n.Client.Close(); err != nil {
		return err
	}
	return n.Server.Stop()
}

func getPrimitiveId(proxyID driverapi.ProxyId) primitiveapi.PrimitiveId {
	return primitiveapi.PrimitiveId{
		Type:      proxyID.Type,
		Namespace: proxyID.Namespace,
		Name:      proxyID.Name,
	}
}
