// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	"github.com/atomix/atomix-go-framework/pkg/atomix/time"
)

// ServiceType is a gossip service type name
type ServiceType string

// Service is a gossip service
type Service interface{}

// Replica is a service replica interface
type Replica interface {
	ID() ServiceId
	Clock() time.Clock
	Read(ctx context.Context, key string) (*Object, error)
	ReadAll(ctx context.Context, ch chan<- Object) error
	Update(ctx context.Context, object *Object) error
}
