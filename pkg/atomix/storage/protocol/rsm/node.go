// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
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
func (n *Node) RegisterService(t ServiceType, f NewServiceFunc) {
	n.registry.Register(t, f)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting protocol")

	member, ok := n.Cluster.Member()
	if !ok {
		return errors.NewUnavailable("not a member of the cluster")
	}

	err := n.protocol.Start(n.Cluster, n.registry)
	if err != nil {
		return err
	}

	err = member.Serve(
		cluster.WithService(func(server *grpc.Server) {
			RegisterPartitionServiceServer(server, &Server{Protocol: n.protocol})
		}),
		cluster.WithService(func(server *grpc.Server) {
			protocolapi.RegisterProtocolConfigServiceServer(server, protocol.NewServer(n.Cluster))
		}))
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
