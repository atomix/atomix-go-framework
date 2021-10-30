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

package meta

import (
	"errors"
	primitives "github.com/atomix/atomix-api/go/atomix/primitive/extensions/primitive"
	services "github.com/atomix/atomix-api/go/atomix/primitive/extensions/service"
	sessions "github.com/atomix/atomix-api/go/atomix/primitive/extensions/session"
	//nolint:staticcheck
	"github.com/lyft/protoc-gen-star"
)

// GetServiceType gets the name extension from the given service
func GetServiceType(service pgs.Service) (services.ServiceType, error) {
	var serviceType services.ServiceType
	ok, err := service.Extension(services.E_Type, &serviceType)
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, errors.New("no atomix.service.type set")
	}
	return serviceType, nil
}

// GetPrimitiveType gets the name extension from the given service
func GetPrimitiveType(service pgs.Service) (string, error) {
	var primitiveType string
	ok, err := service.Extension(primitives.E_Type, &primitiveType)
	if err != nil {
		return "", err
	} else if !ok {
		return "", errors.New("no atomix.primitive.type set")
	}
	return primitiveType, nil
}

// GetOperationType gets the optype extension from the given method
func GetOperationType(method pgs.Method) (sessions.OperationType, error) {
	var operationType sessions.OperationType
	ok, err := method.Extension(sessions.E_Operation, &operationType)
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, errors.New("no atomix.session.operation set")
	}
	return operationType, nil
}
