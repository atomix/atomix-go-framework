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

// CodegenMeta is the metadata for the code generator
type CodegenMeta struct {
	Generator GeneratorMeta
	Location  LocationMeta
	Package   PackageMeta
	Imports   []PackageMeta
}

// GeneratorMeta is the metadata for the code generator
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
	Name   string
	Path   string
	Alias  string
	Import bool
}

// ServiceMeta is the metadata for a service
type ServiceMeta struct {
	Type    ServiceTypeMeta
	Comment string
	Methods []MethodMeta
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

// ServiceTypeMeta is metadata for a service type
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
	Name     string
	Comment  string
	Request  RequestMeta
	Response ResponseMeta
}

// MessageMeta is the metadata for a message
type MessageMeta struct {
	Type TypeMeta
}

// RequestMeta is the type metadata for a message
type RequestMeta struct {
	MessageMeta
	IsUnary  bool
	IsStream bool
}

// ResponseMeta is the type metadata for a message
type ResponseMeta struct {
	MessageMeta
	IsUnary  bool
	IsStream bool
}
