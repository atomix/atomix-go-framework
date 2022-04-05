// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	"github.com/atomix/atomix-go-framework/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/server"
	"google.golang.org/grpc"
)

var log = logging.GetLogger("atomix", "broker")

// NewBroker creates a new broker node
func NewBroker(cluster cluster.Cluster, opts ...Option) *Broker {
	options := applyOptions(opts...)
	return &Broker{
		Server: server.NewServer(cluster),
		namespace: options.namespace,
	}
}

// Broker is a broker node
type Broker struct {
	*server.Server
	namespace string
}

// Start starts the node
func (n *Broker) Start() error {
	server := NewServer(newPrimitiveRegistry(n.namespace))
	n.Server.RegisterService(func(s *grpc.Server) {
		brokerapi.RegisterBrokerServer(s, server)
	})
	if err := n.Server.Start(); err != nil {
		return err
	}
	return nil
}

// Stop stops the node
func (n *Broker) Stop() error {
	if err := n.Server.Stop(); err != nil {
		return err
	}
	return nil
}
