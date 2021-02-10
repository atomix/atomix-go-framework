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
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "proxy")

// NewNode creates a new server node
func NewNode(coordinator *cluster.Replica, cluster cluster.Cluster) *Node {
	return &Node{
		Node:    proxy.NewNode(coordinator, cluster),
		Cluster: cluster,
		Client:  NewClient(cluster),
	}
}

// Node is an Atomix node
type Node struct {
	proxy.Node
	Cluster cluster.Cluster
	Client  *Client
}

// Start starts the node
func (n *Node) Start() error {
	n.Services().RegisterService(func(server *grpc.Server) {
		RegisterPrimitiveServer(server, n.Client)
	})
	if err := n.Node.Start(); err != nil {
		return err
	}
	if err := n.Client.Connect(); err != nil {
		return err
	}
	return nil
}

// Stop stops the node
func (n *Node) Stop() error {
	if err := n.Client.Close(); err != nil {
		return err
	}
	return n.Node.Stop()
}
