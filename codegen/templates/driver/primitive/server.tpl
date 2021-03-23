{{- $server := printf "%sProxyServer" .Primitive.Name }}
{{- $registry := printf "%sProxyRegistry" .Primitive.Name }}
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
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

// New{{ $server }} creates a new {{ $server }}
func New{{ $server }}(registry *{{ $registry }}) {{ $service }} {
	return &{{ $server }}{
		registry: registry,
		log:      logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}),
	}
}

{{- $primitive := .Primitive }}
type {{ $server }} struct {
	registry *{{ $registry }}
	log      logging.Logger
}

{{- range .Primitive.Methods }}
{{- $method := . }}
{{ if and .Request.IsDiscrete .Response.IsDiscrete }}
func (s *{{ $server }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	proxy, err := s.registry.GetProxy(request{{ template "field" .Request.Headers }}.PrimitiveID)
	if err != nil {
	    s.log.Warnf("{{ .Request.Type.Name }} %+v failed: %v", request, err)
		return nil, err
	}
	return proxy.{{ .Name }}(ctx, request)
}
{{ else if .Response.IsStream }}
func (s *{{ $server }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
	proxy, err := s.registry.GetProxy(request{{ template "field" .Request.Headers }}.PrimitiveID)
	if err != nil {
	    s.log.Warnf("{{ .Request.Type.Name }} %+v failed: %v", request, err)
		return err
	}
	return proxy.{{ .Name }}(request, srv)
}
{{ end }}
{{- end }}
