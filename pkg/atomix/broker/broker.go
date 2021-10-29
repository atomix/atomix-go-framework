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

package broker

import (
	"fmt"
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	"github.com/atomix/atomix-go-sdk/pkg/atomix/logging"
	"google.golang.org/grpc"
	"net"
)

var log = logging.GetLogger("atomix", "broker")

// NewBroker creates a new broker node
func NewBroker(opts ...Option) *Broker {
	options := applyOptions(opts...)
	return &Broker{
		port: options.port,
	}
}

// Broker is a broker node
type Broker struct {
	port   int
	server *grpc.Server
}

// Start starts the node
func (n *Broker) Start() error {
	log.Info("Starting broker server")
	server := grpc.NewServer()
	brokerapi.RegisterBrokerServer(server, NewServer(newPrimitiveRegistry()))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", defaultPort))
	if err != nil {
		return err
	}
	return server.Serve(lis)
}

// Stop stops the node
func (n *Broker) Stop() error {
	log.Info("Stopping broker server")
	n.server.GracefulStop()
	return nil
}
