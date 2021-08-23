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
	"fmt"
	"github.com/atomix/atomix-api/go/atomix/primitive/extensions/operation"
	"github.com/atomix/atomix-api/go/atomix/primitive/extensions/partition"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/gossip/primitive"
	"github.com/golang/protobuf/proto"
	"github.com/lyft/protoc-gen-star"
	"github.com/lyft/protoc-gen-star/lang/go"
	"path"
	"path/filepath"
	"strings"
)

// NewContext creates a new metadata context
func NewContext(ctx pgsgo.Context) *Context {
	return &Context{
		ctx: ctx,
	}
}

// Context is the code generation context
type Context struct {
	ctx pgsgo.Context
}

// GetFilePath returns the output path for the given entity
func (c *Context) GetFilePath(entity pgs.Entity, file string) string {
	path := c.ctx.Params().OutputPath()
	if path == "" {
		path = c.ctx.OutputPath(entity).Dir().String()
	}
	return filepath.Join(path, file)
}

// GetFilePath returns the output path for the given entity
func (c *Context) GetTemplatePath(file string) string {
	return file
}

// GetPackageMeta extracts the package metadata for the given entity
func (c *Context) GetPackageMeta(entity pgs.Entity) PackageMeta {
	pkgPath := c.ctx.ImportPath(entity).String()
	pkgMapping, ok := pgsgo.MappedImport(c.ctx.Params(), entity.File().InputPath().String())
	if ok {
		pkgPath = pkgMapping
	}
	pkgName := filepath.Base(pkgPath)
	if pkgName == "map" {
		pkgName = "_map"
	}
	alias := pkgName
	if alias == "log" {
		alias = "_log"
	}
	imported := pkgPath != pgsgo.ImportPath(c.ctx.Params())
	return PackageMeta{
		Name:   pkgName,
		Path:   pkgPath,
		Alias:  alias,
		Import: imported,
	}
}

func (c *Context) GetStateMeta(packages map[string]pgs.Package) (*StateMeta, error) {
	stateType := c.ctx.Params().Str("state")
	if stateType != "" {
		msgType, ok := c.findMessage(stateType, packages)
		if !ok {
			return nil, errors.NewNotFound("could not find state message '%s'", stateType)
		}
		stateTypeMeta := c.GetMessageTypeMeta(msgType)
		keyField, err := c.GetStateKeyFieldMeta(msgType)
		if err != nil {
			return nil, err
		}
		digestField, err := c.GetStateDigestFieldMeta(msgType)
		if err != nil {
			return nil, err
		}
		return &StateMeta{
			IsDiscrete:   true,
			IsContinuous: false,
			Type:         stateTypeMeta,
			Key:          keyField,
			Digest:       digestField,
		}, nil
	}

	entryType := c.ctx.Params().Str("entry")
	if entryType != "" {
		msgType, ok := c.findMessage(entryType, packages)
		if !ok {
			return nil, errors.NewNotFound("could not find entry message '%s'", entryType)
		}
		entryTypeMeta := c.GetMessageTypeMeta(msgType)
		keyField, err := c.GetStateKeyFieldMeta(msgType)
		if err != nil {
			return nil, err
		}
		digestField, err := c.GetStateDigestFieldMeta(msgType)
		if err != nil {
			return nil, err
		}
		return &StateMeta{
			IsDiscrete:   false,
			IsContinuous: true,
			Type:         entryTypeMeta,
			Key:          keyField,
			Digest:       digestField,
		}, nil
	}
	return nil, nil
}

func (c *Context) findMessage(typeName string, packages map[string]pgs.Package) (pgs.Message, bool) {
	for _, pkg := range packages {
		for _, file := range pkg.Files() {
			for _, msg := range file.Messages() {
				if msg.FullyQualifiedName() == typeName {
					return msg, true
				}
			}
		}
	}
	return nil, false
}

// ParseTypeString parses the given type string into type metadata
func (c *Context) ParseTypeString(t string) (TypeMeta, error) {
	parts := strings.Split(t, ".")
	if len(parts) == 1 {
		basePath := pgsgo.ImportPath(c.ctx.Params())
		baseName := path.Base(basePath)
		return TypeMeta{
			Name: t,
			Package: PackageMeta{
				Name:  baseName,
				Path:  basePath,
				Alias: baseName,
			},
		}, nil
	}

	if len(parts) != 2 {
		return TypeMeta{}, fmt.Errorf("'%s' is not a valid type", t)
	}

	pkgPath, typeName := parts[0], parts[1]
	pkgName := path.Base(pkgPath)
	return TypeMeta{
		Name: typeName,
		Package: PackageMeta{
			Name:  pkgName,
			Path:  pkgPath,
			Alias: pkgName,
		},
	}, nil
}

// FindMessage finds a message from its type metadata
func (c *Context) FindMessage(entity pgs.Entity, typeMeta TypeMeta) (pgs.Message, bool) {
	for _, message := range entity.File().Messages() {
		messageTypeMeta := c.GetMessageTypeMeta(message)
		if messageTypeMeta.Package.Path == typeMeta.Package.Path && messageTypeMeta.Name == typeMeta.Name {
			return message, true
		}
	}

	for _, file := range entity.Imports() {
		for _, message := range file.Messages() {
			messageTypeMeta := c.GetMessageTypeMeta(message)
			if messageTypeMeta.Package.Path == typeMeta.Package.Path && messageTypeMeta.Name == typeMeta.Name {
				return message, true
			}
		}
	}
	return nil, false
}

// GetMessageTypeMeta extracts the type metadata for the given message
func (c *Context) GetMessageTypeMeta(message pgs.Message) TypeMeta {
	return TypeMeta{
		Name:      pgsgo.PGGUpperCamelCase(message.Name()).String(),
		Package:   c.GetPackageMeta(message),
		IsMessage: true,
	}
}

func getProtoTypeName(protoType pgs.ProtoType) string {
	switch protoType {
	case pgs.BytesT:
		return "[]byte"
	case pgs.StringT:
		return "string"
	case pgs.Int32T:
		return "int32"
	case pgs.Int64T:
		return "int64"
	case pgs.UInt32T:
		return "uint32"
	case pgs.UInt64T:
		return "uint64"
	case pgs.FloatT:
		return "float32"
	case pgs.DoubleT:
		return "float64"
	case pgs.BoolT:
		return "bool"
	}
	return ""
}

// GetFieldName computes the name for the given field
func (c *Context) GetFieldName(field pgs.Field) string {
	customName, err := GetCustomName(field)
	if err != nil {
		panic(err)
	} else if customName != nil {
		return *customName
	}
	embed, err := GetEmbed(field)
	if err != nil {
		panic(err)
	} else if embed != nil && *embed {
		return pgsgo.PGGUpperCamelCase(field.Type().Embed().Name()).String()
	}
	name := field.Name()
	if name == "size" {
		name = "size_"
	}
	return pgsgo.PGGUpperCamelCase(name).String()
}

// GetRawFieldTypeMeta extracts the raw type metadata for the given field
func (c *Context) GetRawFieldTypeMeta(field pgs.Field) TypeMeta {
	if field.Type().IsMap() {
		return c.GetMapFieldTypeMeta(field)
	}
	if field.Type().IsRepeated() {
		return c.GetRepeatedFieldTypeMeta(field)
	}
	if field.Type().IsEmbed() {
		return c.GetMessageFieldTypeMeta(field)
	}
	if field.Type().IsEnum() {
		return c.GetEnumFieldTypeMeta(field)
	}
	protoType := field.Type().ProtoType()
	return TypeMeta{
		Name:     getProtoTypeName(field.Type().ProtoType()),
		Package:  c.GetPackageMeta(field),
		IsScalar: true,
		IsBytes:  protoType == pgs.BytesT,
		IsString: protoType == pgs.StringT,
		IsInt32:  protoType == pgs.Int32T,
		IsInt64:  protoType == pgs.Int64T,
		IsUint32: protoType == pgs.UInt32T,
		IsUint64: protoType == pgs.UInt64T,
		IsFloat:  protoType == pgs.FloatT,
		IsDouble: protoType == pgs.DoubleT,
		IsBool:   protoType == pgs.BoolT,
	}
}

// GetFieldTypeMeta extracts the type metadata for the given field
func (c *Context) GetFieldTypeMeta(field pgs.Field) TypeMeta {
	if field.Type().IsMap() {
		return c.GetMapFieldTypeMeta(field)
	}
	if field.Type().IsRepeated() {
		return c.GetRepeatedFieldTypeMeta(field)
	}
	if field.Type().IsEmbed() {
		return c.GetMessageFieldTypeMeta(field)
	}
	if field.Type().IsEnum() {
		return c.GetEnumFieldTypeMeta(field)
	}

	protoType := field.Type().ProtoType()
	castType, err := GetCastType(field)
	if err != nil {
		panic(err)
	} else if castType != nil {
		return TypeMeta{
			Name:     *castType,
			Package:  c.GetPackageMeta(field),
			IsScalar: true,
			IsCast:   true,
			IsBytes:  protoType == pgs.BytesT,
			IsString: protoType == pgs.StringT,
			IsInt32:  protoType == pgs.Int32T,
			IsInt64:  protoType == pgs.Int64T,
			IsUint32: protoType == pgs.UInt32T,
			IsUint64: protoType == pgs.UInt64T,
			IsFloat:  protoType == pgs.FloatT,
			IsDouble: protoType == pgs.DoubleT,
			IsBool:   protoType == pgs.BoolT,
		}
	}
	return TypeMeta{
		Name:     getProtoTypeName(field.Type().ProtoType()),
		Package:  c.GetPackageMeta(field),
		IsScalar: true,
		IsBytes:  protoType == pgs.BytesT,
		IsString: protoType == pgs.StringT,
		IsInt32:  protoType == pgs.Int32T,
		IsInt64:  protoType == pgs.Int64T,
		IsUint32: protoType == pgs.UInt32T,
		IsUint64: protoType == pgs.UInt64T,
		IsFloat:  protoType == pgs.FloatT,
		IsDouble: protoType == pgs.DoubleT,
		IsBool:   protoType == pgs.BoolT,
	}
}

// GetMessageFieldTypeMeta extracts the type metadata for the given message field
func (c *Context) GetMessageFieldTypeMeta(field pgs.Field) TypeMeta {
	var fieldType string
	castType, err := GetCastType(field)
	if err != nil {
		panic(err)
	} else if castType != nil {
		fieldType = *castType
	}

	customType, err := GetCustomType(field)
	if err != nil {
		panic(err)
	} else if customType != nil {
		fieldType = *customType
	} else if fieldType == "" {
		fieldType = pgsgo.PGGUpperCamelCase(field.Type().Embed().Name()).String()
	}

	pointer := true
	nullable, err := GetNullable(field)
	if err != nil {
		panic(err)
	} else if nullable != nil {
		pointer = *nullable
	}

	return TypeMeta{
		Name:      fieldType,
		Package:   c.GetPackageMeta(field.Type().Embed()),
		IsMessage: true,
		IsPointer: pointer,
	}
}

// GetRepeatedFieldTypeMeta extracts the type metadata for the given repeated field
func (c *Context) GetRepeatedFieldTypeMeta(field pgs.Field) TypeMeta {
	elementTypeMeta := c.GetFieldElementTypeMeta(field)
	elementTypeMeta.IsRepeated = true
	return elementTypeMeta
}

// GetMapFieldTypeMeta extracts the type metadata for the given map field
func (c *Context) GetMapFieldTypeMeta(field pgs.Field) TypeMeta {
	keyTypeMeta := c.GetFieldKeyTypeMeta(field)
	valueTypeMeta := c.GetFieldValueTypeMeta(field)
	return TypeMeta{
		Name:      "map",
		Package:   c.GetPackageMeta(field),
		IsMap:     true,
		KeyType:   &keyTypeMeta,
		ValueType: &valueTypeMeta,
	}
}

// GetFieldKeyTypeMeta extracts the key type metadata for the given field
func (c *Context) GetFieldKeyTypeMeta(field pgs.Field) TypeMeta {
	castKey, err := GetCastKey(field)
	if err != nil {
		panic(err)
	} else if castKey != nil {
		keyTypeMeta, err := c.ParseTypeString(*castKey)
		if err != nil {
			panic(err)
		}
		return keyTypeMeta
	}
	if field.Type().Key().IsEmbed() {
		return c.GetMessageTypeMeta(field.Type().Key().Embed())
	}
	protoType := field.Type().Element().ProtoType()
	return TypeMeta{
		Name:     getProtoTypeName(field.Type().Key().ProtoType()),
		Package:  c.GetPackageMeta(field),
		IsScalar: true,
		IsBytes:  protoType == pgs.BytesT,
		IsString: protoType == pgs.StringT,
		IsInt32:  protoType == pgs.Int32T,
		IsInt64:  protoType == pgs.Int64T,
		IsUint32: protoType == pgs.UInt32T,
		IsUint64: protoType == pgs.UInt64T,
		IsFloat:  protoType == pgs.FloatT,
		IsDouble: protoType == pgs.DoubleT,
		IsBool:   protoType == pgs.BoolT,
	}
}

// GetFieldValueTypeMeta extracts the value type metadata for the given field
func (c *Context) GetFieldValueTypeMeta(field pgs.Field) TypeMeta {
	castValue, err := GetCastValue(field)
	if err != nil {
		panic(err)
	} else if castValue != nil {
		valueTypeMeta, err := c.ParseTypeString(*castValue)
		if err != nil {
			panic(err)
		}
		return valueTypeMeta
	}
	if field.Type().Element().IsEmbed() {
		return c.GetMessageTypeMeta(field.Type().Element().Embed())
	}
	protoType := field.Type().Element().ProtoType()
	return TypeMeta{
		Name:     getProtoTypeName(field.Type().Element().ProtoType()),
		Package:  c.GetPackageMeta(field),
		IsScalar: true,
		IsBytes:  protoType == pgs.BytesT,
		IsString: protoType == pgs.StringT,
		IsInt32:  protoType == pgs.Int32T,
		IsInt64:  protoType == pgs.Int64T,
		IsUint32: protoType == pgs.UInt32T,
		IsUint64: protoType == pgs.UInt64T,
		IsFloat:  protoType == pgs.FloatT,
		IsDouble: protoType == pgs.DoubleT,
		IsBool:   protoType == pgs.BoolT,
	}
}

// GetFieldElementTypeMeta extracts the element type metadata for the given field
func (c *Context) GetFieldElementTypeMeta(field pgs.Field) TypeMeta {
	castValue, err := GetCastValue(field)
	if err != nil {
		panic(err)
	} else if castValue != nil {
		elementTypeMeta, err := c.ParseTypeString(*castValue)
		if err != nil {
			panic(err)
		}
		return elementTypeMeta
	}
	if field.Type().Element().IsEmbed() {
		return c.GetMessageTypeMeta(field.Type().Element().Embed())
	}
	protoType := field.Type().Element().ProtoType()
	return TypeMeta{
		Name:     getProtoTypeName(field.Type().Element().ProtoType()),
		Package:  c.GetPackageMeta(field),
		IsScalar: true,
		IsBytes:  protoType == pgs.BytesT,
		IsString: protoType == pgs.StringT,
		IsInt32:  protoType == pgs.Int32T,
		IsInt64:  protoType == pgs.Int64T,
		IsUint32: protoType == pgs.UInt32T,
		IsUint64: protoType == pgs.UInt64T,
		IsFloat:  protoType == pgs.FloatT,
		IsDouble: protoType == pgs.DoubleT,
		IsBool:   protoType == pgs.BoolT,
	}
}

// GetEnumFieldTypeMeta extracts the type metadata for the given enum field
func (c *Context) GetEnumFieldTypeMeta(field pgs.Field) TypeMeta {
	values := make([]TypeMeta, 0, len(field.Type().Enum().Values()))
	for _, value := range field.Type().Enum().Values() {
		values = append(values, c.GetEnumValueTypeMeta(value))
	}
	return TypeMeta{
		Name:    pgsgo.PGGUpperCamelCase(field.Type().Enum().Name()).String(),
		Package: c.GetPackageMeta(field.Type().Enum()),
		IsEnum:  true,
		Values:  values,
	}
}

// GetEnumValueTypeMeta extracts the type metadata for the given enum value
func (c *Context) GetEnumValueTypeMeta(enumValue pgs.EnumValue) TypeMeta {
	return TypeMeta{
		Name:        pgsgo.PGGUpperCamelCase(enumValue.Name()).String(),
		Package:     c.GetPackageMeta(enumValue),
		IsEnumValue: true,
	}
}

// GetStateKeyFieldMeta extracts the metadata for the state key field in the given message
func (c *Context) GetStateKeyFieldMeta(message pgs.Message) (*FieldRefMeta, error) {
	return c.findAnnotatedField(message, primitive.E_Key)
}

// GetStateDigestFieldMeta extracts the metadata for the state digest field in the given message
func (c *Context) GetStateDigestFieldMeta(message pgs.Message) (*FieldRefMeta, error) {
	return c.findAnnotatedField(message, primitive.E_Digest)
}

// GetPartitionKeyField extracts the metadata for the partitionkey field in the given message
func (c *Context) GetPartitionKeyFieldMeta(message pgs.Message) (*FieldRefMeta, error) {
	return c.findAnnotatedField(message, partition.E_Key)
}

// GetPartitionRangeField extracts the metadata for the partitionrange field in the given message
func (c *Context) GetPartitionRangeFieldMeta(message pgs.Message) (*FieldRefMeta, error) {
	return c.findAnnotatedField(message, partition.E_Range)
}

// GetAggregateFields extracts the metadata for aggregated fields in the given message
func (c *Context) GetAggregateFields(message pgs.Message) ([]AggregatorMeta, error) {
	return c.findAggregateFields(message)
}

func (c *Context) findAggregateFields(message pgs.Message) ([]AggregatorMeta, error) {
	fields := make([]AggregatorMeta, 0)
	for _, field := range message.Fields() {
		var aggregate operation.AggregateStrategy
		ok, err := field.Extension(operation.E_Aggregate, &aggregate)
		if err != nil {
			return nil, err
		} else if ok {
			fields = append(fields, AggregatorMeta{
				FieldRefMeta: FieldRefMeta{
					Field: FieldMeta{
						Type: c.GetFieldTypeMeta(field),
						Path: []PathMeta{
							{
								Name: c.GetFieldName(field),
								Type: c.GetFieldTypeMeta(field),
							},
						},
					},
				},
				IsChooseFirst: aggregate == operation.AggregateStrategy_CHOOSE_FIRST,
				IsAppend:      aggregate == operation.AggregateStrategy_APPEND,
				IsSum:         aggregate == operation.AggregateStrategy_SUM,
			})
		} else if field.Type().IsEmbed() {
			children, err := c.findAggregateFields(field.Type().Embed())
			if err != nil {
				return nil, err
			} else {
				for _, child := range children {
					fields = append(fields, AggregatorMeta{
						FieldRefMeta: FieldRefMeta{
							Field: FieldMeta{
								Type: child.Field.Type,
								Path: append([]PathMeta{
									{
										Name: c.GetFieldName(field),
										Type: c.GetFieldTypeMeta(field),
									},
								}, child.Field.Path...),
							},
						},
						IsChooseFirst: child.IsChooseFirst,
						IsAppend:      child.IsAppend,
						IsSum:         child.IsSum,
					})
				}
			}
		}
	}
	return fields, nil
}

func (c *Context) findAnnotatedField(message pgs.Message, extension *proto.ExtensionDesc) (*FieldRefMeta, error) {
	for _, field := range message.Fields() {
		var isAnnotatedField bool
		ok, err := field.Extension(extension, &isAnnotatedField)
		if err != nil {
			return nil, err
		} else if ok {
			return &FieldRefMeta{
				Field: FieldMeta{
					Type: c.GetFieldTypeMeta(field),
					Path: []PathMeta{
						{
							Name: c.GetFieldName(field),
							Type: c.GetFieldTypeMeta(field),
						},
					},
				},
			}, nil
		} else if field.Type().IsEmbed() {
			child, err := c.findAnnotatedField(field.Type().Embed(), extension)
			if err != nil {
				return nil, err
			} else if child != nil {
				return &FieldRefMeta{
					Field: FieldMeta{
						Type: child.Field.Type,
						Path: append([]PathMeta{
							{
								Name: c.GetFieldName(field),
								Type: c.GetFieldTypeMeta(field),
							},
						}, child.Field.Path...),
					},
				}, nil
			}
		}
	}
	return nil, nil
}
