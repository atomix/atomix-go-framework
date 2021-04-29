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
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "protocol", "gossip")

// NewNode creates a new server node
func NewNode(cluster cluster.Cluster, scheme time.Scheme) *Node {
	return &Node{
		Cluster:  cluster,
		scheme:   scheme,
		registry: NewRegistry(),
	}
}

// Node is an Atomix node
type Node struct {
	Cluster  cluster.Cluster
	scheme   time.Scheme
	registry *Registry
}

// RegisterServer registers a server
func (n *Node) RegisterServer(f RegisterServerFunc) {
	n.registry.RegisterServer(f)
}

// RegisterService registers a service type
func (n *Node) RegisterService(t ServiceType, f NewServiceFunc) {
	n.registry.RegisterService(t, f)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting server")

	manager := newManager(n.Cluster, n.scheme, n.registry)
	servers := n.registry.GetServers()
	services := make([]cluster.Service, len(servers))
	for i, f := range servers {
		services[i] = func(f RegisterServerFunc) func(s *grpc.Server) {
			return func(s *grpc.Server) {
				f(s, manager)
			}
		}(f)
	}
	services = append(services, RegisterPrimitiveServer)
	services = append(services, func(server *grpc.Server) {
		RegisterGossipServer(server, manager)
	})

	member, ok := n.Cluster.Member()
	if !ok {
		return errors.NewUnavailable("not a member of the cluster")
	}
	err := member.Serve(cluster.WithServices(services...))
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
	return n.Cluster.Close()
}
