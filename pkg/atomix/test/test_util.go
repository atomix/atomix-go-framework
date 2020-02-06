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

package test

import (
	"fmt"
	"github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-client/pkg/client/primitive"
	netutil "github.com/atomix/go-client/pkg/client/util/net"
	"github.com/atomix/go-framework/pkg/atomix"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-local/pkg/atomix/local"
	"net"
)

const basePort = 5000

// StartTestNode starts a single test node
func StartTestNode() (primitive.Partition, *atomix.Node) {
	for port := basePort; port < basePort+100; port++ {
		address := netutil.Address(fmt.Sprintf("localhost:%d", port))
		lis, err := net.Listen("tcp", string(address))
		if err != nil {
			continue
		}
		node := local.NewNode(lis, node.GetRegistry(), []*controller.PartitionId{{Partition: 1}})
		node.Start()

		ch := make(chan struct{})
		go func() {
			<-ch
			node.Stop()
		}()
		return primitive.Partition{ID: 1, Address: address}, node
	}
	panic("cannot find open port")
}
