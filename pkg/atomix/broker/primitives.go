// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	brokerapi "github.com/atomix/atomix-api/go/atomix/management/broker"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// newPrimitiveRegistry creates a new primitive registry
func newPrimitiveRegistry(namespace string) *PrimitiveRegistry {
	return &PrimitiveRegistry{
		namespace:  namespace,
		primitives: make(map[brokerapi.PrimitiveId]brokerapi.PrimitiveAddress),
	}
}

// PrimitiveRegistry is a primitive registry
// The registry is not thread safe!
type PrimitiveRegistry struct {
	namespace  string
	primitives map[brokerapi.PrimitiveId]brokerapi.PrimitiveAddress
}

func (r *PrimitiveRegistry) AddPrimitive(id brokerapi.PrimitiveId, primitive brokerapi.PrimitiveAddress) error {
	if id.Namespace == "" {
		id.Namespace = r.namespace
	}
	if _, ok := r.primitives[id]; ok {
		return errors.NewAlreadyExists("primitive '%s' already exists", id)
	}
	r.primitives[id] = primitive
	return nil
}

func (r *PrimitiveRegistry) RemovePrimitive(id brokerapi.PrimitiveId) error {
	if id.Namespace == "" {
		id.Namespace = r.namespace
	}
	if _, ok := r.primitives[id]; ok {
		return errors.NewNotFound("primitive '%s' not found", id)
	}
	delete(r.primitives, id)
	return nil
}

func (r *PrimitiveRegistry) LookupPrimitive(id brokerapi.PrimitiveId) (brokerapi.PrimitiveAddress, error) {
	if id.Namespace == "" {
		id.Namespace = r.namespace
	}
	primitive, ok := r.primitives[id]
	if !ok {
		return brokerapi.PrimitiveAddress{}, errors.NewNotFound("primitive '%s' not found", id)
	}
	return primitive, nil
}
