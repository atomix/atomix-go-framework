{{- $server := printf "%sServer" .Generator.Prefix }}
{{- $service := printf "%s.%sServer" .Primitive.Type.Package.Alias .Primitive.Type.Name }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

{{- define "field" }}
{{- $path := .Field.Path }}
{{- range $index, $element := $path -}}
{{- if eq $index 0 -}}
{{- if isLast $path $index -}}
{{- if $element.Type.IsPointer -}}
.Get{{ $element.Name }}()
{{- else -}}
.{{ $element.Name }}
{{- end -}}
{{- else -}}
{{- if $element.Type.IsPointer -}}
.Get{{ $element.Name }}().
{{- else -}}
.{{ $element.Name }}.
{{- end -}}
{{- end -}}
{{- else -}}
{{- if isLast $path $index -}}
{{- if $element.Type.IsPointer -}}
    Get{{ $element.Name }}()
{{- else -}}
    {{ $element.Name -}}
{{- end -}}
{{- else -}}
{{- if $element.Type.IsPointer -}}
    Get{{ $element.Name }}().
{{- else -}}
    {{ $element.Name }}.
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

package {{ .Package.Name }}

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/proxy"
	"google.golang.org/grpc"
	"sync"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

const {{ printf "%sType" .Generator.Prefix }} = {{ .Primitive.Name | quote }}

func RegisterService(node proxy.Node) {
	node.Services().RegisterService(func(s *grpc.Server) {
		server := &Server{
			node:      node,
			instances: make(map[string]{{ $service }}),
            log:       logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}),
		}
		{{ .Primitive.Type.Package.Alias }}.Register{{ .Primitive.Type.Name }}Server(s, server)
	})
}

{{- $primitive := .Primitive }}
type {{ $server }} struct {
	node       proxy.Node
	instances  map[string]{{ $service }}
	mu         sync.RWMutex
	log        logging.Logger
}

func (s *{{ $server }}) getInstance(ctx context.Context, name string) ({{ $service }}, error) {
	s.mu.RLock()
	instance, ok := s.instances[name]
	s.mu.RUnlock()
	if ok {
		return instance, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	instance, ok = s.instances[name]
	if ok {
		return instance, nil
	}

	primitiveType, err := s.node.PrimitiveTypes().GetPrimitiveType({{ printf "%sType" .Generator.Prefix }})
	if err != nil {
		return nil, err
	}

	primitiveMeta, err := s.node.Primitives().GetPrimitive(ctx, name)
	if err != nil {
		return nil, err
	}

	proxy, err := primitiveType.NewProxy()
	if err != nil {
		return nil, err
	}
	instance = proxy.({{ $service }})

	if primitiveMeta.Cached {
		cached, err := primitiveType.NewCacheDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = cached.({{ $service }})
	}

	if primitiveMeta.ReadOnly {
		readOnly, err := primitiveType.NewReadOnlyDecorator(instance)
		if err != nil {
			return nil, err
		}
		instance = readOnly.({{ $service }})
	}

	s.instances[name] = instance
	return instance, nil
}

{{- range .Primitive.Methods }}
{{- $method := . }}
{{ if and .Request.IsDiscrete .Response.IsDiscrete }}
func (s *{{ $server }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	instance, err := s.getInstance(ctx, request{{ template "field" .Request.Headers }}.PrimitiveID)
	if err != nil {
	    s.log.Warnf("{{ .Request.Type.Name }} %+v failed: %v", request, err)
		return nil, err
	}
	return instance.{{ .Name }}(ctx, request)
}
{{ else if .Response.IsStream }}
func (s *{{ $server }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
	instance, err := s.getInstance(srv.Context(), request{{ template "field" .Request.Headers }}.PrimitiveID)
	if err != nil {
	    s.log.Warnf("{{ .Request.Type.Name }} %+v failed: %v", request, err)
		return err
	}
	return instance.{{ .Name }}(request, srv)
}
{{ end }}
{{- end }}
