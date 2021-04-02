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
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "protocol")

// NewNode creates a new node running the given protocol
func NewNode(cluster cluster.Cluster, protocol Protocol) *Node {
	return &Node{
		Cluster:  cluster,
		protocol: protocol,
		registry: NewRegistry(),
	}
}

// Node is an Atomix node
type Node struct {
	Cluster  cluster.Cluster
	protocol Protocol
	registry *Registry
}

// RegisterService registers a primitive service
func (n *Node) RegisterService(t string, f NewServiceFunc) {
	n.registry.Register(t, f)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting protocol")

	member, ok := n.Cluster.Member()
	if !ok {
		return errors.NewUnavailable("not a member of the cluster")
	}
	err := member.Serve(
		cluster.WithService(func(server *grpc.Server) {
			RegisterStorageServiceServer(server, &Server{Protocol: n.protocol})
		}),
		cluster.WithService(func(server *grpc.Server) {
			protocolapi.RegisterProtocolConfigServiceServer(server, protocol.NewServer(n.Cluster))
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
