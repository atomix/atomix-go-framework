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

import "github.com/atomix/atomix-go-sdk/codegen/meta"

// CodegenMeta is the metadata for the code generator
type CodegenMeta struct {
	meta.CodegenMeta
	Primitive PrimitiveMeta
}

// PrimitiveMeta is the metadata for a primitive
type PrimitiveMeta struct {
	ServiceMeta
	Name string
}

// ServiceMeta is the metadata for a service
type ServiceMeta struct {
	meta.ServiceMeta
	Methods []MethodMeta
}

// MethodMeta is the metadata for a primitive method
type MethodMeta struct {
	meta.MethodMeta
	ID          uint32
	Type        MethodTypeMeta
	Scope       MethodScopeMeta
	Partitioner MethodPartitionerMeta
	Request     RequestMeta
	Response    ResponseMeta
}

// MethodScopeMeta is the metadata for a method scope
type MethodScopeMeta struct {
	IsPartition bool
	IsGlobal    bool
}

// MethodPartitionerMeta is the metadata for partitioning requests
type MethodPartitionerMeta struct {
	IsName       bool
	IsHash       bool
	IsRange      bool
	IsRandom     bool
	IsRoundRobin bool
}

// RequestMeta is the type metadata for a message
type RequestMeta struct {
	meta.RequestMeta
	PartitionKey   *meta.FieldRefMeta
	PartitionRange *meta.FieldRefMeta
}

// ResponseMeta is the type metadata for a message
type ResponseMeta struct {
	meta.ResponseMeta
	Aggregates []AggregatorMeta
}

// AggregatorMeta is the metadata for response aggregation
type AggregatorMeta struct {
	meta.FieldRefMeta
	IsChooseFirst bool
	IsAppend      bool
	IsSum         bool
}

// MethodTypeMeta is the metadata for a store method type
type MethodTypeMeta struct {
	IsCommand bool
	IsQuery   bool
	IsSync    bool
	IsAsync   bool
}
