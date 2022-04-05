// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"fmt"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"google.golang.org/grpc"
)

// NewMember returns a new local group member
func NewMember(network Network, config protocolapi.ProtocolReplica) *Member {
	return &Member{
		Replica: NewReplica(network, config),
	}
}

// Service is a peer-to-peer primitive service
type Service func(*grpc.Server)

// Member is a local group member
type Member struct {
	*Replica
	server *grpc.Server
}

// Serve begins serving the local member
func (m *Member) Serve(opts ...ServeOption) error {
	options := applyServeOptions(opts...)

	m.server = grpc.NewServer(options.grpcOptions...)
	for _, service := range options.services {
		service(m.server)
	}

	lis, err := m.network.Listen(fmt.Sprintf(":%d", m.Port))
	if err != nil {
		return err
	}

	go func() {
		err := m.server.Serve(lis)
		if err != nil {
			fmt.Println(err)
		}
	}()
	return nil
}

// Stop stops the local member serving
func (m *Member) Stop() error {
	m.server.Stop()
	return nil
}
