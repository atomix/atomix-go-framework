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

type CodegenMeta struct {
	Generator GeneratorMeta
	Location  LocationMeta
	Package   PackageMeta
	Imports   []PackageMeta
	Primitive PrimitiveMeta
}

type GeneratorMeta struct {
	Prefix string
}

// LocationMeta is the location of a code file
type LocationMeta struct {
	Path string
	File string
}

// PackageMeta is the package for a code file
type PackageMeta struct {
	Name  string
	Path  string
	Alias string
}

// TypeMeta is the metadata for a store type
type TypeMeta struct {
	Name        string
	Package     PackageMeta
	IsPointer   bool
	IsScalar    bool
	IsCast      bool
	IsMessage   bool
	IsMap       bool
	IsRepeated  bool
	IsEnum      bool
	IsEnumValue bool
	IsBytes     bool
	IsString    bool
	IsInt32     bool
	IsInt64     bool
	IsUint32    bool
	IsUint64    bool
	IsFloat     bool
	IsDouble    bool
	IsBool      bool
	KeyType     *TypeMeta
	ValueType   *TypeMeta
	Values      []TypeMeta
}

// PrimitiveMeta is the metadata for a primitive
type PrimitiveMeta struct {
	ServiceMeta
	Name  string
	State StateMeta
}

// ServiceMeta is the metadata for a service
type ServiceMeta struct {
	Type    ServiceTypeMeta
	Comment string
	Methods []MethodMeta
}

type StateMeta struct {
	Value *StateTypeMeta
	Entry *StateTypeMeta
}

type StateTypeMeta struct {
	Type   TypeMeta
	Key    *FieldRefMeta
	Digest *FieldRefMeta
}

type ServiceTypeMeta struct {
	Name    string
	Package PackageMeta
}

// FieldRefMeta is metadata for a field reference
type FieldRefMeta struct {
	Field FieldMeta
}

// FieldMeta is metadata for a field
type FieldMeta struct {
	Type TypeMeta
	Path []PathMeta
}

// PathMeta is metadata for a field path
type PathMeta struct {
	Name string
	Type TypeMeta
}

// MethodMeta is the metadata for a primitive method
type MethodMeta struct {
	Name        string
	Type        MethodTypeMeta
	Comment     string
	Scope       MethodScopeMeta
	Partitioner MethodPartitionerMeta
	Request     RequestMeta
	Response    ResponseMeta
}

// ScopeMeta is the metadata for a method scope
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

// MessageMeta is the metadata for a message
type MessageMeta struct {
	Type TypeMeta
}

// RequestMeta is the type metadata for a message
type RequestMeta struct {
	MessageMeta
	Header         FieldRefMeta
	PartitionKey   *FieldRefMeta
	PartitionRange *FieldRefMeta
	IsDiscrete     bool
	IsStream       bool
}

// ResponseMeta is the type metadata for a message
type ResponseMeta struct {
	MessageMeta
	Header     FieldRefMeta
	Aggregates []AggregatorMeta
	IsDiscrete bool
	IsStream   bool
}

type AggregatorMeta struct {
	FieldRefMeta
	IsChooseFirst bool
	IsAppend      bool
	IsSum         bool
}

// MethodTypeMeta is the metadata for a store method type
type MethodTypeMeta struct {
	IsCommand bool
	IsQuery   bool
	IsAsync   bool
}
