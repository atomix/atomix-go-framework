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

package coordinator

import (
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "driver")

// NewNode creates a new driver node
func NewNode(cluster cluster.Cluster) *Node {
	return &Node{
		Server: server.NewServer(cluster),
	}
}

// Node is a driver node
type Node struct {
	*server.Server
}

// Start starts the node
func (n *Node) Start() error {
	n.RegisterService(func(s *grpc.Server) {
		server := NewServer(newDriverRegistry(), newPrimitiveRegistry())
		driverapi.RegisterProxyManagementServiceServer(s, server)
		driverapi.RegisterDriverManagementServiceServer(s, server)
	})
	return n.Server.Start()
}
