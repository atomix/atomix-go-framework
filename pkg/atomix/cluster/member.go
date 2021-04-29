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

package cluster

import (
	"fmt"
	protocolapi "github.com/atomix/atomix-api/go/atomix/protocol"
	"google.golang.org/grpc"
	"net"
)

// NewMember returns a new local group member
func NewMember(config protocolapi.ProtocolReplica) *Member {
	return &Member{
		Replica: NewReplica(config),
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

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", m.Port))
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
