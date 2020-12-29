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
	"fmt"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"google.golang.org/grpc"
	"net"
)

// NewNode creates a new node running the given protocol
func NewNode(cluster *cluster.Cluster, protocol Protocol, opts ...NodeOption) *Node {
	node := &Node{
		Cluster:  cluster,
		protocol: protocol,
		registry: NewRegistry(),
		startCh:  make(chan error),
	}
	(&defaultOption{}).apply(node)
	for _, opt := range opts {
		opt.apply(node)
	}
	return node
}

// NodeOption is an option for constructing a Node
type NodeOption interface {
	apply(*Node)
}

// defaultOption is a node option which applies initial defaults
type defaultOption struct{}

func (o *defaultOption) apply(node *Node) {
	node.port = 5678
	node.listener = tcpListener{}
}

// WithLocal sets the node to local mode for testing
func WithLocal(lis net.Listener) NodeOption {
	return &localOption{lis}
}

type localOption struct {
	listener net.Listener
}

func (o *localOption) apply(node *Node) {
	node.listener = localListener{o.listener}
}

// WithPort sets the port on the node
func WithPort(port int) NodeOption {
	return &portOption{port: port}
}

type portOption struct {
	port int
}

func (o *portOption) apply(node *Node) {
	node.port = o.port
}

// Node is an Atomix node
type Node struct {
	Cluster  *cluster.Cluster
	protocol Protocol
	registry Registry
	port     int
	listener listener
	server   *grpc.Server
	startCh  chan error
}

// RegisterService registers a primitive service
func (n *Node) RegisterService(t string, primitive PrimitiveService) {
	n.registry.Register(t, primitive)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting protocol")
	err := n.protocol.Start(n.Cluster, n.registry)
	if err != nil {
		return err
	}

	lis, err := n.listener.listen(n)
	if err != nil {
		return err
	}

	// Set the ready file to indicate startup of the protocol is complete.
	ready := util.NewFileReady()
	_ = ready.Set()

	go func() {
		_ = n.run(lis)
	}()
	return nil
}

// Run runs the server
func (n *Node) run(lis net.Listener) error {
	log.Info("Starting gRPC server")
	n.server = grpc.NewServer()
	RegisterStorageServiceServer(n.server, &Server{Protocol: n.protocol})
	return n.server.Serve(lis)
}

// Stop stops the node
func (n *Node) Stop() error {
	n.server.GracefulStop()
	if err := n.protocol.Stop(); err != nil {
		return err
	}
	return nil
}

type listener interface {
	listen(*Node) (net.Listener, error)
}

type tcpListener struct{}

func (l tcpListener) listen(node *Node) (net.Listener, error) {
	log.Infof("Listening on port %d", node.port)
	return net.Listen("tcp", fmt.Sprintf(":%d", node.port))
}

type localListener struct {
	listener net.Listener
}

func (l localListener) listen(node *Node) (net.Listener, error) {
	return l.listener, nil
}
