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

package proxy

import (
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "proxy")

// NewNode creates a new server node
func NewNode(cluster *cluster.Cluster) *Node {
	return &Node{
		Cluster:  cluster,
		client:   NewClient(cluster),
		registry: NewRegistry(),
		startCh:  make(chan error),
	}
}

// Node is an Atomix node
type Node struct {
	Cluster  *cluster.Cluster
	client   *Client
	registry Registry
	startCh  chan error
}

// RegisterService registers a primitive service
func (n *Node) RegisterServer(t string, primitive PrimitiveServer) {
	n.registry.Register(t, primitive)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting protocol")

	servers := n.registry.GetPrimitives()
	services := make([]cluster.Service, len(servers))
	for i, server := range servers {
		services[i] = func(s *grpc.Server) {
			server.RegisterServer(s, n.client)
		}
	}

	err := n.Cluster.Member().Serve(cluster.WithServices(services...))
	if err != nil {
		return err
	}

	err = n.client.Connect()
	if err != nil {
		return err
	}

	// Set the ready file to indicate startup of the protocol is complete.
	ready := util.NewFileReady()
	_ = ready.Set()
	return nil
}

// Stop stops the node
func (n *Node) Stop() error {
	if err := n.client.Close(); err != nil {
		return err
	}
	return n.Cluster.Close()
}
