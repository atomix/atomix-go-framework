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
	"context"
)

// ServiceType is a gossip service type name
type ServiceType string

// ServiceID is a gossip service identifier
type ServiceID string

// Service is a gossip service
type Service interface{}

// Replica is a gossip replica interface
type Replica interface {
	Service() Service
	Read(ctx context.Context, key string) (*Object, error)
	Update(ctx context.Context, object *Object) error
	Clone(ctx context.Context, ch chan<- Object) error
}
