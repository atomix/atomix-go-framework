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
	"github.com/gogo/protobuf/gogoproto"
	gogoprotobuf "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/lyft/protoc-gen-star"
)

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
