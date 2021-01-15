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
	proxyapi "github.com/atomix/api/go/atomix/proxy"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"github.com/atomix/go-framework/pkg/atomix/util"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "proxy")

// NewNode creates a new server node
func NewNode(cluster *cluster.Cluster) *Node {
	return &Node{
		Cluster:  cluster,
		client:   NewClient(cluster),
		registry: NewRegistry(),
	}
}

// Node is an Atomix node
type Node struct {
	Cluster  *cluster.Cluster
	client   *Client
	registry Registry
}

// RegisterService registers a primitive service
func (n *Node) RegisterProxy(proxy RegisterProxyFunc) {
	n.registry.RegisterProxy(proxy)
}

// Start starts the node
func (n *Node) Start() error {
	log.Info("Starting protocol")

	var newProxyService = func(f RegisterProxyFunc) cluster.Service {
		return func(s *grpc.Server) {
			f(s, n.client)
		}
	}
	proxies := n.registry.GetProxies()
	services := make([]cluster.Service, 0, len(proxies)+2)
	for _, proxyFunc := range proxies {
		services = append(services, newProxyService(proxyFunc))
	}
	services = append(services, newProxyService(RegisterPrimitiveServer))
	services = append(services, func(s *grpc.Server) {
		proxyapi.RegisterProxyConfigServiceServer(s, proxy.NewServer(n.Cluster))
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
	if err := n.client.Close(); err != nil {
		return err
	}
	return n.Cluster.Close()
}
