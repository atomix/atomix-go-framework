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

package client

import (
	"context"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"google.golang.org/grpc"
)

type PrimitiveType string

// NewPrimitiveClient creates a new primitive client
func NewPrimitiveClient(primitiveType PrimitiveType, name string, conn *grpc.ClientConn) PrimitiveClient {
	return &primitiveClient{
		primitiveType: primitiveType,
		name:          name,
		client:        primitiveapi.NewPrimitiveServiceClient(conn),
	}
}

// PrimitiveClient is the client interface for a primitive
type PrimitiveClient interface {
	// Name returns the primitive name
	Name() string

	// Type returns the primitive type
	Type() PrimitiveType

	// Create creates the primitive
	Create(ctx context.Context) error

	// Close closes the primitive
	Close(ctx context.Context) error

	// Delete deletes the primitive state from the cluster
	Delete(ctx context.Context) error
}

// primitiveClient is an implementation of the PrimitiveClient interface
type primitiveClient struct {
	primitiveType PrimitiveType
	name          string
	client        primitiveapi.PrimitiveServiceClient
}

func (p *primitiveClient) Name() string {
	return p.name
}

func (p *primitiveClient) Type() PrimitiveType {
	return p.primitiveType
}

func (p *primitiveClient) Create(ctx context.Context) error {
	request := &primitiveapi.OpenRequest{
		Header: primitiveapi.RequestHeader{
			PrimitiveID: primitiveapi.PrimitiveId{
				Type: string(p.primitiveType),
				Name: p.name,
			},
		},
	}
	_, err := p.client.Open(ctx, request)
	return err
}

func (p *primitiveClient) Close(ctx context.Context) error {
	request := &primitiveapi.CloseRequest{
		Header: primitiveapi.RequestHeader{
			PrimitiveID: primitiveapi.PrimitiveId{
				Type: string(p.primitiveType),
				Name: p.name,
			},
		},
	}
	_, err := p.client.Close(ctx, request)
	return err
}

func (p *primitiveClient) Delete(ctx context.Context) error {
	request := &primitiveapi.CloseRequest{
		Header: primitiveapi.RequestHeader{
			PrimitiveID: primitiveapi.PrimitiveId{
				Type: string(p.primitiveType),
				Name: p.name,
			},
		},
		Delete: true,
	}
	_, err := p.client.Close(ctx, request)
	return err
}
