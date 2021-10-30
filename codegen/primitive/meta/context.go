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
	"github.com/atomix/atomix-api/go/atomix/primitive/extensions/operation"
	"github.com/atomix/atomix-api/go/atomix/primitive/extensions/partition"
	"github.com/atomix/atomix-go-sdk/codegen/meta"
	"github.com/lyft/protoc-gen-star"
	"github.com/lyft/protoc-gen-star/lang/go"
)

// NewContext creates a new metadata context
func NewContext(ctx pgsgo.Context) *Context {
	return &Context{
		Context: meta.NewContext(ctx),
		ctx:     ctx,
	}
}

// Context is the code generation context
type Context struct {
	*meta.Context
	ctx pgsgo.Context
}

// GetPartitionKeyFieldMeta extracts the metadata for the partitionkey field in the given message
func (c *Context) GetPartitionKeyFieldMeta(message pgs.Message) (*meta.FieldRefMeta, error) {
	return c.FindAnnotatedField(message, partition.E_Key)
}

// GetPartitionRangeFieldMeta extracts the metadata for the partitionrange field in the given message
func (c *Context) GetPartitionRangeFieldMeta(message pgs.Message) (*meta.FieldRefMeta, error) {
	return c.FindAnnotatedField(message, partition.E_Range)
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
				FieldRefMeta: meta.FieldRefMeta{
					Field: meta.FieldMeta{
						Type: c.GetFieldTypeMeta(field),
						Path: []meta.PathMeta{
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
						FieldRefMeta: meta.FieldRefMeta{
							Field: meta.FieldMeta{
								Type: child.Field.Type,
								Path: append([]meta.PathMeta{
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
