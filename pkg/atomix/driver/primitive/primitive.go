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

package primitive

import (
	"github.com/atomix/api/go/atomix/management/driver"
	driverapi "github.com/atomix/api/go/atomix/management/driver"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"google.golang.org/grpc"
)

// PrimitiveID is a primitive identifier
type PrimitiveID interface {
	GetType() string
	GetNamespace() string
	GetName() string
}

// PrimitiveType is an interface for defining a primitive type
type PrimitiveType interface {
	// Name returns the primitive type name
	Name() string
	// Register registers the primitive server
	RegisterServer(server *grpc.Server)
	// AddProxy adds a proxy to the primitive server
	AddProxy(proxy driver.ProxyConfig) error
	// RemoveProxy removes a proxy from the primitive server
	RemoveProxy(id driver.ProxyId) error
}

func getPrimitiveId(proxyID driverapi.ProxyId) primitiveapi.PrimitiveId {
	return primitiveapi.PrimitiveId{
		Type:      proxyID.Type,
		Namespace: proxyID.Namespace,
		Name:      proxyID.Name,
	}
}
