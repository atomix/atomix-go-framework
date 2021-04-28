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
	driverapi "github.com/atomix/api/go/atomix/management/driver"
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
	// RegisterServer registers the primitive server
	RegisterServer(server *grpc.Server)
	// AddProxy adds a proxy to the primitive server
	AddProxy(id driverapi.ProxyId, options driverapi.ProxyOptions) error
	// RemoveProxy removes a proxy from the primitive server
	RemoveProxy(id driverapi.ProxyId) error
}
