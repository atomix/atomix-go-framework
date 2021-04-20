{{- $primitive := .Primitive }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

{{- $proxy := printf "%sProxyServer" .Generator.Prefix }}
{{- $service := printf "%s.%sServer" .Primitive.Type.Package.Alias .Primitive.Type.Name }}

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

{{- define "var" }}
{{- range $index, $element := .Field.Path -}}
{{- if eq $index 0 -}}
{{ .Name | toLowerCamel }}
{{- else -}}
{{ .Name }}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "ref" -}}
{{- if not .Field.Type.IsPointer }}&{{ end }}
{{- end }}

{{- define "val" -}}
{{- if .Field.Type.IsPointer }}*{{ end }}
{{- end }}

{{- define "optype" }}
{{- if .Type.IsCommand -}}
Command
{{- else if .Type.IsQuery -}}
Query
{{- end -}}
{{- end }}
package {{ .Package.Name }}

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/driver/proxy/rsm"
	storage "github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- range .Primitive.Methods }}
	{{- if .Scope.IsGlobal }}
	{{ import "github.com/atomix/go-framework/pkg/atomix/util/async" }}
	{{- end }}
	{{- if .Request.IsStream }}
	{{ import "io" }}
	{{- end }}
	{{- if .Response.IsStream }}
	{{ import "streams" "github.com/atomix/go-framework/pkg/atomix/stream" }}
	{{- end }}
	{{- end }}
)

const {{ printf "%sType" .Generator.Prefix }} = {{ .Primitive.Name | quote }}
{{ $root := . }}
const (
    {{- range .Primitive.Methods }}
    {{ (printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel }} = {{ .Name | quote }}
    {{- end }}
)

// New{{ $proxy }} creates a new {{ $proxy }}
func New{{ $proxy }}(client *rsm.Client) {{ $service }} {
	return &{{ $proxy }}{
		Client: client,
		log:    logging.GetLogger("atomix", "counter"),
	}
}

type {{ $proxy }} struct {
	*rsm.Client
	log logging.Logger
}

{{- range .Primitive.Methods }}
{{- $name := ((printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel) }}
{{- $method := . }}
{{ if and .Request.IsDiscrete .Response.IsDiscrete }}
func (s *{{ $proxy }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, errors.Proto(err)
	}

	{{- if .Scope.IsPartition }}
	{{- if .Request.PartitionKey }}
	partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
	{{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
	partition := s.PartitionBy(partitionKey)
	{{- else }}
	{{- if .Request.PartitionKey.Field.Type.IsString }}
    partition := s.PartitionBy([]byte(partitionKey))
    {{- else }}
    partition := s.PartitionBy([]byte(partitionKey.String()))
    {{- end }}
	{{- end }}
	{{- else if .Request.PartitionRange }}
	partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
	{{- else }}
    partition := s.PartitionBy([]byte(request{{ template "field" .Request.Headers }}.PrimitiveID.String()))
	{{- end }}

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request{{ template "field" .Request.Headers }}.PrimitiveID.Namespace,
		Name:      request{{ template "field" .Request.Headers }}.PrimitiveID.Name,
	}
	output, err := partition.Do{{ template "optype" . }}(ctx, service, {{ $name }}, input)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := &{{ template "type" .Response.Type }}{}
	err = proto.Unmarshal(output, response)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, errors.Proto(err)
	}
	{{- else if .Scope.IsGlobal }}
	partitions := s.Partitions()
	{{- $aggregates := false }}
    {{- range .Response.Aggregates }}
    {{- $aggregates = true }}
    {{- end }}

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request{{ template "field" .Request.Headers }}.PrimitiveID.Namespace,
		Name:      request{{ template "field" .Request.Headers }}.PrimitiveID.Name,
	}
	{{- if .Response.Aggregates }}
	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].Do{{ template "optype" . }}(ctx, service, {{ $name }}, input)
	})
	{{- else }}
	err = async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].Do{{ template "optype" . }}(ctx, service, {{ $name }}, input)
		return err
	})
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, errors.Proto(err)
	}

	{{- if .Response.Aggregates }}
	responses := make([]{{ template "type" $method.Response.Type }}, 0, len(outputs))
	for _, output := range outputs {
	    var response {{ template "type" $method.Response.Type }}
        err := proto.Unmarshal(output.([]byte), &response)
        if err != nil {
            s.log.Errorf("Request {{ $method.Request.Type.Name }} failed: %v", err)
            return nil, errors.Proto(err)
        }
        responses = append(responses, response)
	}
	{{- end }}

	response := &{{ template "type" $method.Response.Type }}{}
    {{- range .Response.Aggregates }}
    {{- if .IsChooseFirst }}
    response{{ template "field" . }} = responses[0]{{ template "field" . }}
    {{- else if .IsAppend }}
    for _, r := range responses {
        response{{ template "field" . }} = append(response{{ template "field" . }}, r{{ template "field" . }}...)
    }
    {{- else if .IsSum }}
    for _, r := range responses {
        response{{ template "field" . }} += r{{ template "field" . }}
    }
    {{- end }}
	{{- end }}
	{{- end }}
	s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
	return response, nil
}
{{ else if .Response.IsStream }}
func (s *{{ $proxy }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
    s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
	input, err := proto.Marshal(request)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
        return errors.Proto(err)
	}

	stream := streams.NewBufferedStream()
	{{- if .Scope.IsPartition }}
	{{- if .Request.PartitionKey }}
	partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
	{{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
	partition := s.PartitionBy(partitionKey)
	{{- else }}
	{{- if .Request.PartitionKey.Field.Type.IsString }}
    partition := s.PartitionBy([]byte(partitionKey))
    {{- else }}
    partition := s.PartitionBy([]byte(partitionKey.String()))
    {{- end }}
	{{- end }}
	{{- else if .Request.PartitionRange }}
	partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
	{{- else }}
    partition := s.PartitionBy([]byte(request{{ template "field" .Request.Headers }}.PrimitiveID.String()))
	{{- end }}

	service := storage.ServiceId{
		Type:      Type,
		Namespace: request{{ template "field" .Request.Headers }}.PrimitiveID.Namespace,
		Name:      request{{ template "field" .Request.Headers }}.PrimitiveID.Name,
	}
	err = partition.Do{{ template "optype" . }}Stream(srv.Context(), service, {{ $name }}, input, stream)
	{{- else if .Scope.IsGlobal }}
	service := storage.ServiceId{
		Type:      Type,
		Namespace: request{{ template "field" .Request.Headers }}.PrimitiveID.Namespace,
		Name:      request{{ template "field" .Request.Headers }}.PrimitiveID.Name,
	}
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].Do{{ template "optype" . }}Stream(srv.Context(), service, {{ $name }}, input, stream)
	})
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return errors.Proto(err)
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", result.Error)
			return errors.Proto(result.Error)
		}

		response := &{{ template "type" .Response.Type }}{}
        err = proto.Unmarshal(result.Value.([]byte), response)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return errors.Proto(err)
        }

		s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
		if err = srv.Send(response); err != nil {
            s.log.Errorf("Response {{ .Response.Type.Name }} failed: %v", err)
			return errors.Proto(err)
		}
	}
	s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
	return nil
}
{{ end }}
{{- end }}
