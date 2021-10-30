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

package session

import (
	"fmt"
	services "github.com/atomix/atomix-api/go/atomix/primitive/extensions/service"
	sessions "github.com/atomix/atomix-api/go/atomix/primitive/extensions/session"
	"github.com/atomix/atomix-go-sdk/codegen/meta"
	sessionmeta "github.com/atomix/atomix-go-sdk/codegen/session/meta"
	"github.com/atomix/atomix-go-sdk/codegen/template"
	"github.com/lyft/protoc-gen-star"
	"github.com/lyft/protoc-gen-star/lang/go"
)

const moduleName = "atomix"

// NewModule creates a new proto module
func NewModule(plugin string, templates map[string]string) pgs.Module {
	return &Module{
		ModuleBase: &pgs.ModuleBase{},
		plugin:     plugin,
		templates:  templates,
	}
}

// Module is the code generation module
type Module struct {
	*pgs.ModuleBase
	ctx       *sessionmeta.Context
	plugin    string
	templates map[string]string
}

// Name returns the module name
func (m *Module) Name() string {
	return moduleName
}

// InitContext initializes the module context
func (m *Module) InitContext(c pgs.BuildContext) {
	m.ModuleBase.InitContext(c)
	m.ctx = sessionmeta.NewContext(pgsgo.InitContext(c.Parameters()))
}

func (m *Module) isPluginEnabled() bool {
	return m.Parameters().Str("plugin") == m.plugin
}

// Execute executes the code generator
func (m *Module) Execute(targets map[string]pgs.File, packages map[string]pgs.Package) []pgs.Artifact {
	if !m.isPluginEnabled() {
		return m.Artifacts()
	}
	for _, target := range targets {
		m.executeTarget(target, packages)
	}
	return m.Artifacts()
}

func (m *Module) executeTarget(target pgs.File, packages map[string]pgs.Package) {
	println(target.File().InputPath())
	for _, service := range target.Services() {
		m.executeService(service, packages)
	}
}

// executeService generates a store from a Protobuf service
//nolint:gocyclo
func (m *Module) executeService(service pgs.Service, packages map[string]pgs.Package) {
	serviceType, err := sessionmeta.GetServiceType(service)
	if err != nil {
		return
	}

	if serviceType != services.ServiceType_SESSION {
		return
	}

	primitiveType, err := sessionmeta.GetPrimitiveType(service)
	if err != nil {
		return
	}

	importsSet := make(map[string]meta.PackageMeta)
	var addImport = func(t *meta.TypeMeta) {
		if t.Package.Import {
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
	methods := make([]sessionmeta.MethodMeta, 0)
	for _, method := range service.Methods() {
		// Get the operation type for the method.
		operationType, err := sessionmeta.GetOperationType(method)
		if err != nil {
			panic(err)
		}

		methodTypeMeta := sessionmeta.MethodTypeMeta{
			IsOpen:  operationType == sessions.OperationType_OPEN,
			IsClose: operationType == sessions.OperationType_CLOSE,
		}

		requestMeta := sessionmeta.RequestMeta{
			RequestMeta: meta.RequestMeta{
				MessageMeta: meta.MessageMeta{
					Type: m.ctx.GetMessageTypeMeta(method.Input()),
				},
				IsUnary:  !method.ClientStreaming(),
				IsStream: method.ClientStreaming(),
			},
		}
		addImport(&requestMeta.Type)

		switch operationType {
		case sessions.OperationType_OPEN:
			primitiveIDMeta, err := m.ctx.GetPrimitiveIDFieldMeta(method.Input())
			if err != nil {
				panic(err)
			}
			optionsMeta, err := m.ctx.GetOptionsFieldMeta(method.Input())
			if err != nil {
				panic(err)
			}
			requestMeta.Open = &sessionmeta.OpenRequestMeta{
				PrimitiveID: primitiveIDMeta,
				Options:     optionsMeta,
			}
		case sessions.OperationType_CLOSE:
			sessionIDMeta, err := m.ctx.GetSessionIDFieldMeta(method.Input())
			if err != nil {
				panic(err)
			}
			requestMeta.Close = &sessionmeta.CloseRequestMeta{
				SessionID: sessionIDMeta,
			}
		}

		// Generate output metadata from the output type.
		responseMeta := sessionmeta.ResponseMeta{
			ResponseMeta: meta.ResponseMeta{
				MessageMeta: meta.MessageMeta{
					Type: m.ctx.GetMessageTypeMeta(method.Output()),
				},
				IsUnary:  !method.ServerStreaming(),
				IsStream: method.ServerStreaming(),
			},
		}
		addImport(&responseMeta.Type)

		switch operationType {
		case sessions.OperationType_OPEN:
			sessionIDMeta, err := m.ctx.GetSessionIDFieldMeta(method.Input())
			if err != nil {
				panic(err)
			}
			responseMeta.Open = &sessionmeta.OpenResponseMeta{
				SessionID: sessionIDMeta,
			}
		case sessions.OperationType_CLOSE:
			responseMeta.Close = &sessionmeta.CloseResponseMeta{}
		}

		methodMeta := sessionmeta.MethodMeta{
			MethodMeta: meta.MethodMeta{
				Name:     method.Name().UpperCamelCase().String(),
				Comment:  method.SourceCodeInfo().LeadingComments(),
				Request:  requestMeta.RequestMeta,
				Response: responseMeta.ResponseMeta,
			},
			Type:     methodTypeMeta,
			Request:  requestMeta,
			Response: responseMeta,
		}

		methods = append(methods, methodMeta)
	}

	// Generate a list of imports from the deduplicated package metadata set.
	imports := make([]meta.PackageMeta, 0, len(importsSet))
	for _, importPkg := range importsSet {
		imports = append(imports, importPkg)
	}

	primitiveMeta := sessionmeta.PrimitiveMeta{
		Name: primitiveType,
		ServiceMeta: sessionmeta.ServiceMeta{
			ServiceMeta: meta.ServiceMeta{
				Type: meta.ServiceTypeMeta{
					Name:    pgsgo.PGGUpperCamelCase(service.Name()).String(),
					Package: m.ctx.GetPackageMeta(service),
				},
				Comment: service.SourceCodeInfo().LeadingComments(),
			},
			Methods: methods,
		},
	}

	// Generate the store metadata.
	meta := sessionmeta.CodegenMeta{
		CodegenMeta: meta.CodegenMeta{
			Generator: meta.GeneratorMeta{
				Prefix: m.BuildContext.Parameters().Str("prefix"),
			},
			Location: meta.LocationMeta{},
			Package:  m.ctx.GetPackageMeta(service),
			Imports:  imports,
		},
		Primitive: primitiveMeta,
	}

	for f, t := range m.templates {
		m.OverwriteGeneratorTemplateFile(m.ctx.GetFilePath(service, f), template.NewTemplate(m.ctx.GetTemplatePath(t), importsSet), meta)
	}
}
