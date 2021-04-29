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

package server

import (
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/node"
	"github.com/atomix/atomix-go-framework/pkg/atomix/util"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "server")

// Node is an interface for proxy nodes
type Node interface {
	node.Node
	Services() *ServiceRegistry
}

// NewServer creates a new server
func NewServer(cluster cluster.Cluster) *Server {
	return &Server{
		Cluster:  cluster,
		services: NewServiceRegistry(),
	}
}

// Server is a base server
type Server struct {
	Cluster  cluster.Cluster
	services *ServiceRegistry
}

// Services returns the service registry
func (s *Server) Services() *ServiceRegistry {
	return s.services
}

// RegisterService registers a service
func (s *Server) RegisterService(service RegisterServiceFunc) {
	s.services.RegisterService(service)
}

// Start starts the node
func (s *Server) Start() error {
	log.Info("Starting server")

	servers := s.services.GetServices()
	services := make([]cluster.Service, len(servers))
	for i, f := range servers {
		services[i] = func(f RegisterServiceFunc) func(s *grpc.Server) {
			return func(s *grpc.Server) {
				f(s)
			}
		}(f)
	}

	member, ok := s.Cluster.Member()
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
func (s *Server) Stop() error {
	return s.Cluster.Close()
}
