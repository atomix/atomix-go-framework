// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package primitive

import (
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
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
