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
	"github.com/atomix/api/go/atomix/primitive"
	"github.com/gogo/protobuf/gogoproto"
	gogoprotobuf "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/lyft/protoc-gen-star"
)

// GetPrimitiveType gets the name extension from the given service
func GetPrimitiveType(service pgs.Service) (string, error) {
	var primitiveType string
	ok, err := service.Extension(primitive.E_Type, &primitiveType)
	if err != nil {
		return "", err
	} else if !ok {
		return "", errors.New("no atomix.primitive.type set")
	}
	return primitiveType, nil
}

// GetPartition gets the partition extension from the given service
func GetPartition(service pgs.Service) (bool, error) {
	var partition bool
	ok, err := service.Extension(primitive.E_Partition, &partition)
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
	ok, err := method.Extension(primitive.E_Async, &async)
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
	ok, err := method.Extension(primitive.E_Opname, &opName)
	if err != nil {
		return "", err
	} else if !ok {
		return method.Name().String(), nil
	}
	return opName, nil
}

// GetOperationType gets the optype extension from the given method
func GetOperationType(method pgs.Method) (primitive.OperationType, error) {
	var operationType primitive.OperationType
	ok, err := method.Extension(primitive.E_Optype, &operationType)
	if err != nil {
		return 0, err
	} else if !ok {
		return 0, errors.New("no atomix.primitive.optype set")
	}
	return operationType, nil
}

// GetPartitionStrategy gets the partition strategy extension from the given method
func GetPartitionStrategy(method pgs.Method) (*primitive.PartitionStrategy, error) {
	var strategy primitive.PartitionStrategy
	ok, err := method.Extension(primitive.E_Partitionby, &strategy)
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
