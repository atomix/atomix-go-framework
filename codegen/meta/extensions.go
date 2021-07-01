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
	operations "github.com/atomix/atomix-api/go/atomix/primitive/extensions/operation"
	partitions "github.com/atomix/atomix-api/go/atomix/primitive/extensions/partition"
	services "github.com/atomix/atomix-api/go/atomix/primitive/extensions/service"
	"github.com/gogo/protobuf/gogoproto"
	gogoprotobuf "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/lyft/protoc-gen-star"
)

// GetPrimitiveType gets the name extension from the given service
func GetPrimitiveType(service pgs.Service) (string, error) {
	var primitiveType string
	ok, err := service.Extension(services.E_Type, &primitiveType)
	if err != nil {
		return "", err
	} else if !ok {
		return "", errors.New("no atomix.primitive.type set")
	}
	return primitiveType, nil
}

// GetPartitioned gets the partitioned extension from the given service
func GetPartitioned(service pgs.Service) (bool, error) {
	var partition bool
	ok, err := service.Extension(services.E_Partitioned, &partition)
	if err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	return partition, nil
}

// GetAsync gets the partition extension from the given service
func GetAsync(method pgs.Method) (bool, error) {
	var async bool
	ok, err := method.Extension(operations.E_Async, &async)
	if err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	return async, nil
}

// GetOperationName gets the name extension from the given method
func GetOperationName(method pgs.Method) (string, error) {
	var opName string
	ok, err := method.Extension(operations.E_Name, &opName)
	if err != nil {
		return "", err
	} else if !ok {
		return method.Name().String(), nil
	}
	return opName, nil
}

// GetOperationID gets the id extension from the given method
func GetOperationID(method pgs.Method) (uint32, error) {
	var operationID uint32
	ok, err := method.Extension(operations.E_Id, &operationID)
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, errors.New("no atomix.primitive.operation.id set")
	}
	return operationID, nil
}

// GetOperationType gets the optype extension from the given method
func GetOperationType(method pgs.Method) (operations.OperationType, error) {
	var operationType operations.OperationType
	ok, err := method.Extension(operations.E_Type, &operationType)
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, errors.New("no atomix.primitive.optype set")
	}
	return operationType, nil
}

// GetHeaders gets the headers extension from the given field
func GetHeaders(field pgs.Field) (bool, error) {
	var headers bool
	ok, err := field.Extension(operations.E_Headers, &headers)
	if err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	return headers, nil
}

// GetPartitionStrategy gets the partition strategy extension from the given method
func GetPartitionStrategy(method pgs.Method) (*partitions.PartitionStrategy, error) {
	var strategy partitions.PartitionStrategy
	ok, err := method.Extension(partitions.E_Strategy, &strategy)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &strategy, nil
}

func getExtensionDesc(extension *gogoprotobuf.ExtensionDesc) *proto.ExtensionDesc {
	return &proto.ExtensionDesc{
		ExtendedType:  (*descriptor.FieldOptions)(nil),
		ExtensionType: extension.ExtensionType,
		Field:         extension.Field,
		Name:          extension.Name,
		Tag:           extension.Tag,
		Filename:      extension.Filename,
	}
}

// GetEmbed gets the embed extension from the given field
func GetEmbed(field pgs.Field) (*bool, error) {
	var embed bool
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Embed), &embed)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &embed, nil
}

// GetCastType gets the casttype extension from the given field
func GetCastType(field pgs.Field) (*string, error) {
	var castType string
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Casttype), &castType)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &castType, nil
}

// GetCastKey gets the castkey extension from the given field
func GetCastKey(field pgs.Field) (*string, error) {
	var castKey string
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Castkey), &castKey)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &castKey, nil
}

// GetCastValue gets the castvalue extension from the given field
func GetCastValue(field pgs.Field) (*string, error) {
	var castValue string
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Castvalue), &castValue)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &castValue, nil
}

// GetCustomName gets the customname extension from the given field
func GetCustomName(field pgs.Field) (*string, error) {
	var customName string
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Customname), &customName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &customName, nil
}

// GetCustomType gets the customtype extension from the given field
func GetCustomType(field pgs.Field) (*string, error) {
	var customType string
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Customtype), &customType)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &customType, nil
}

// GetNullable gets the nullable extension from the given field
func GetNullable(field pgs.Field) (*bool, error) {
	var nullable bool
	ok, err := field.Extension(getExtensionDesc(gogoproto.E_Nullable), &nullable)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &nullable, nil
}
