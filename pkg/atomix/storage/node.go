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

package storage

import (
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "storage")

// NewNode creates a new node running the given protocol
func NewNode(cluster *cluster.Cluster, protocol Protocol) *Node {
	return &Node{
		Cluster:  cluster,
		protocol: protocol,
		registry: NewRegistry(),
		startCh:  make(chan error),
	}
}

// Node is an Atomix node
type Node struct {
	Cluster  *cluster.Cluster
	protocol Protocol
	registry Registry
	startCh  chan error
}

// RegisterService registers a primitive service
func (n *Node) RegisterService(t string, primitive PrimitiveService) {
	n.registry.Register(t, primitive)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting protocol")

	err := n.Cluster.Member().Serve(cluster.WithService(func(server *grpc.Server) {
		RegisterStorageServiceServer(server, &Server{Protocol: n.protocol})
	}))
	if err != nil {
		return err
	}

	err = n.protocol.Start(n.Cluster, n.registry)
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
	if err := n.protocol.Stop(); err != nil {
		return err
	}
	return n.Cluster.Close()
}
