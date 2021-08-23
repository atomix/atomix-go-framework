// Code generated by atomix-go-framework. DO NOT EDIT.
package {{ .Package.Name }}

{{ $server := printf "%sProxyServer" .Generator.Prefix }}
{{- $registry := printf "%sProxyRegistry" .Generator.Prefix }}
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

import (
	"context"
	driverapi "github.com/atomix/atomix-api/go/atomix/management/driver"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/atomix/atomix-go-framework/pkg/atomix/driver/env"
	"google.golang.org/grpc/metadata"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

var log = logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }})

{{- $type := printf "%sType" .Generator.Prefix }}
const {{ $type }} = {{ .Primitive.Name | quote }}

// New{{ $server }} creates a new {{ $server }}
func New{{ $server }}(registry *{{ $registry }}) {{ $service }} {
	return &{{ $server }}{
		registry: registry,
	}
}

{{- $primitive := .Primitive }}
type {{ $server }} struct {
	registry *{{ $registry }}
	env      env.DriverEnv
}

{{- range .Primitive.Methods }}
{{- $method := . }}
{{ if and .Request.IsUnary .Response.IsUnary }}
func (s *{{ $server }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive headers"))
	}
	primitiveName, ok := rsm.GetPrimitiveName(md)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive header"))
	}
	proxyID := driverapi.ProxyId{
	    Type: {{ $type }},
	    Name: primitiveName,
	}
	proxy, err := s.registry.GetProxy(proxyID)
	if err != nil {
	    log.Warnf("{{ .Request.Type.Name }} %+v failed: %v", request, err)
	    if errors.IsNotFound(err) {
	        return nil, errors.NewUnavailable(err.Error())
	    }
		return nil, err
	}
	return proxy.{{ .Name }}(ctx, request)
}
{{ else if .Response.IsStream }}
func (s *{{ $server }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive headers"))
	}
	primitiveName, ok := rsm.GetPrimitiveName(md)
	if !ok {
		return nil, errors.Proto(errors.NewInvalid("missing primitive header"))
	}
	proxyID := driverapi.ProxyId{
	    Type: {{ $type }},
	    Name: primitiveName,
	}
	proxy, err := s.registry.GetProxy(proxyID)
	if err != nil {
	    log.Warnf("{{ .Request.Type.Name }} %+v failed: %v", request, err)
	    if errors.IsNotFound(err) {
	        return errors.NewUnavailable(err.Error())
	    }
		return err
	}
	return proxy.{{ .Name }}(request, srv)
}
{{ end }}
{{- end }}
