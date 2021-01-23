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

package codegen

import (
	"fmt"
	operationext "github.com/atomix/api/go/atomix/primitive/extensions/operation"
	partitionext "github.com/atomix/api/go/atomix/primitive/extensions/partition"
	"github.com/atomix/go-framework/codegen/meta"
	"github.com/lyft/protoc-gen-star"
	"github.com/lyft/protoc-gen-star/lang/go"
)

const moduleName = "atomix"

// NewModule creates a new proto module
func NewModule(plugin string, protocol string, templates map[string]string) pgs.Module {
	return &Module{
		ModuleBase: &pgs.ModuleBase{},
		plugin:     plugin,
		protocol:   protocol,
		templates:  templates,
	}
}

// Module is the code generation module
type Module struct {
	*pgs.ModuleBase
	ctx       *meta.Context
	plugin    string
	protocol  string
	templates map[string]string
}

// Name returns the module name
func (m *Module) Name() string {
	return moduleName
}

// InitContext initializes the module context
func (m *Module) InitContext(c pgs.BuildContext) {
	m.ModuleBase.InitContext(c)
	m.ctx = meta.NewContext(pgsgo.InitContext(c.Parameters()))
}

func (m *Module) isPluginEnabled() bool {
	return m.Parameters().Str("plugin") == m.plugin
}

func (m *Module) isProtocolEnabled() bool {
	return m.Parameters().Str("protocol") == m.protocol
}

// Execute executes the code generator
func (m *Module) Execute(targets map[string]pgs.File, packages map[string]pgs.Package) []pgs.Artifact {
	if !m.isPluginEnabled() || !m.isProtocolEnabled() {
		return m.Artifacts()
	}
	for _, target := range targets {
		m.executeTarget(target)
	}
	return m.Artifacts()
}

func (m *Module) executeTarget(target pgs.File) {
	for _, service := range target.Services() {
		m.executeService(service)
	}
}

// executeService generates a store from a Protobuf service
//nolint:gocyclo
func (m *Module) executeService(service pgs.Service) {
	primitiveType, err := meta.GetPrimitiveType(service)
	if err != nil {
		return
	}

	partition, err := meta.GetPartitioned(service)
	if err != nil {
		panic(err)
	}

	importsSet := make(map[string]meta.PackageMeta)
	var addImport = func(t *meta.TypeMeta) {
		if t.Package.Alias != "" {
			baseAlias := t.Package.Alias
			i := 0
			for {
				importPackage, ok := importsSet[t.Package.Alias]
				if ok {
					if importPackage.Path != t.Package.Path {
						t.Package.Alias = fmt.Sprintf("%s%d", baseAlias, i)
					} else {
						break
					}
				} else {
					importsSet[t.Package.Alias] = t.Package
				}
				i++
			}
		}
	}

	// Iterate through the methods on the service and construct method metadata for the template.
	methods := make([]meta.MethodMeta, 0)
	for _, method := range service.Methods() {
		// Get the operation type for the method.
		operationType, err := meta.GetOperationType(method)
		if err != nil {
			panic(err)
		}

		async, err := meta.GetAsync(method)
		if err != nil {
			panic(err)
		}

		methodTypeMeta := meta.MethodTypeMeta{
			IsCommand: operationType == operationext.OperationType_COMMAND,
			IsQuery:   operationType == operationext.OperationType_QUERY,
			IsAsync:   async,
		}

		requestHeaders, err := m.ctx.GetHeadersFieldMeta(method.Input())
		if err != nil {
			panic(err)
		} else if requestHeaders == nil {
			panic("no request headers found on method input " + method.Input().FullyQualifiedName())
		}

		requestMeta := meta.RequestMeta{
			MessageMeta: meta.MessageMeta{
				Type: m.ctx.GetMessageTypeMeta(method.Input()),
			},
			Headers:    *requestHeaders,
			IsDiscrete: !method.ClientStreaming(),
			IsStream:   method.ClientStreaming(),
		}
		addImport(&requestMeta.Type)

		var methodScopeMeta meta.MethodScopeMeta
		var methodPartitionerMeta meta.MethodPartitionerMeta

		partitionStrategy, err := meta.GetPartitionStrategy(method)
		if err != nil {
			panic(err)
		}

		if partition {
			if partitionStrategy != nil {
				switch *partitionStrategy {
				case partitionext.PartitionStrategy_NONE:
					methodScopeMeta = meta.MethodScopeMeta{
						IsGlobal: true,
					}
				case partitionext.PartitionStrategy_HASH:
					partitionKey, err := m.ctx.GetPartitionKeyFieldMeta(method.Input())
					if err != nil {
						panic(err)
					} else if partitionKey == nil {
						panic(fmt.Errorf("method '%s' is annotated with 'atomix.primitive.partitionby = HASH`, but no 'atomix.primitive.partitionkey' annotated field found in request message", method.Name().String()))
					} else {
						requestMeta.PartitionKey = partitionKey
					}
					methodScopeMeta = meta.MethodScopeMeta{
						IsPartition: true,
					}
					methodPartitionerMeta = meta.MethodPartitionerMeta{
						IsHash: true,
					}
				case partitionext.PartitionStrategy_RANGE:
					partitionRange, err := m.ctx.GetPartitionRangeFieldMeta(method.Input())
					if err != nil {
						panic(err)
					} else if partitionRange == nil {
						panic(fmt.Errorf("method '%s' is annotated with 'atomix.primitive.partitionby = RANGE`, but no 'atomix.primitive.partitionrange' annotated field found in request message", method.Name().String()))
					} else {
						requestMeta.PartitionRange = partitionRange
					}
					methodScopeMeta = meta.MethodScopeMeta{
						IsPartition: true,
					}
					methodPartitionerMeta = meta.MethodPartitionerMeta{
						IsRange: true,
					}
				case partitionext.PartitionStrategy_RANDOM:
					methodScopeMeta = meta.MethodScopeMeta{
						IsPartition: true,
					}
					methodPartitionerMeta = meta.MethodPartitionerMeta{
						IsRandom: true,
					}
				case partitionext.PartitionStrategy_ROUND_ROBIN:
					methodScopeMeta = meta.MethodScopeMeta{
						IsPartition: true,
					}
					methodPartitionerMeta = meta.MethodPartitionerMeta{
						IsRoundRobin: true,
					}
				}
			} else {
				methodScopeMeta = meta.MethodScopeMeta{
					IsGlobal: true,
				}
			}
		} else {
			if partitionStrategy != nil {
				panic(fmt.Errorf("method '%s' is annotated with 'atomix.primitive.partitionby`, but service '%s' is not annotated with 'atomix.primitive.partition'", method.Name().String(), service.Name().String()))
			}

			methodScopeMeta = meta.MethodScopeMeta{
				IsPartition: true,
			}
			methodPartitionerMeta = meta.MethodPartitionerMeta{
				IsName: true,
			}
		}

		responseHeaders, err := m.ctx.GetHeadersFieldMeta(method.Output())
		if err != nil {
			panic(err)
		} else if responseHeaders == nil {
			panic("no request headers found on method input " + method.Output().FullyQualifiedName())
		}

		// Generate output metadata from the output type.
		responseMeta := meta.ResponseMeta{
			MessageMeta: meta.MessageMeta{
				Type: m.ctx.GetMessageTypeMeta(method.Output()),
			},
			Headers:    *responseHeaders,
			IsDiscrete: !method.ServerStreaming(),
			IsStream:   method.ServerStreaming(),
		}
		addImport(&responseMeta.Type)

		aggregates, err := m.ctx.GetAggregateFields(method.Output())
		if err != nil {
			panic(err)
		}
		responseMeta.Aggregates = aggregates

		methodMeta := meta.MethodMeta{
			Name:        method.Name().UpperCamelCase().String(),
			Comment:     method.SourceCodeInfo().LeadingComments(),
			Type:        methodTypeMeta,
			Scope:       methodScopeMeta,
			Partitioner: methodPartitionerMeta,
			Request:     requestMeta,
			Response:    responseMeta,
		}

		methods = append(methods, methodMeta)
	}

	// Generate a list of imports from the deduplicated package metadata set.
	imports := make([]meta.PackageMeta, 0, len(importsSet))
	for _, importPkg := range importsSet {
		imports = append(imports, importPkg)
	}

	valueType, err := m.ctx.GetStateValueTypeMeta(service)
	if err != nil {
		panic(err)
	} else if valueType != nil {
		addImport(&valueType.Type)
	}

	entryType, err := m.ctx.GetStateEntryTypeMeta(service)
	if err != nil {
		panic(err)
	} else if entryType != nil {
		addImport(&entryType.Type)
	}

	stateMeta := meta.StateMeta{
		Value: valueType,
		Entry: entryType,
	}

	primitiveMeta := meta.PrimitiveMeta{
		Name: primitiveType,
		ServiceMeta: meta.ServiceMeta{
			Type: meta.ServiceTypeMeta{
				Name:    pgsgo.PGGUpperCamelCase(service.Name()).String(),
				Package: m.ctx.GetPackageMeta(service),
			},
			Comment: service.SourceCodeInfo().LeadingComments(),
			Methods: methods,
		},
		State: stateMeta,
	}

	// Generate the store metadata.
	meta := meta.CodegenMeta{
		Generator: meta.GeneratorMeta{
			Prefix: m.BuildContext.Parameters().Str("prefix"),
		},
		Location:  meta.LocationMeta{},
		Package:   m.ctx.GetPackageMeta(service),
		Imports:   imports,
		Primitive: primitiveMeta,
	}

	for file, template := range m.templates {
		m.OverwriteGeneratorTemplateFile(m.ctx.GetFilePath(service, file), NewTemplate(m.ctx.GetTemplatePath(template), importsSet), meta)
	}
}
