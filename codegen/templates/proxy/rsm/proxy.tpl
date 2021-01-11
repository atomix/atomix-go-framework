{{- $proxy := printf "%sProxy" .Generator.Prefix }}
package {{ .Package.Name }}

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/rsm"
	{{- $added := false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Scope.IsGlobal }}
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $added = false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Response.IsStream }}
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $added = false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Request.IsStream }}
	"io"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	"github.com/atomix/go-framework/pkg/atomix/util/logging"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

const {{ printf "%sType" .Generator.Prefix }} = {{ .Primitive.Name | quote }}
{{ $root := . }}
const (
    {{- range .Primitive.Methods }}
    {{ (printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel }} = {{ .Name | quote }}
    {{- end }}
)

// Register{{ $proxy }} registers the primitive on the given node
func Register{{ $proxy }}(node *rsm.Node) {
	node.RegisterServer({{ printf "%sType" .Generator.Prefix }}, func(server *grpc.Server, client *rsm.Client) {
		{{ .Primitive.Type.Package.Alias }}.Register{{ .Primitive.Type.Name }}Server(server, &{{ $proxy }}{
			Proxy: rsm.NewProxy(client),
			log: logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}),
		})
	})
}

{{- $primitive := .Primitive }}
type {{ $proxy }} struct {
	*rsm.Proxy
	log logging.Logger
}

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

{{- $root := . }}
{{- range .Primitive.Methods }}
{{- $name := ((printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel) }}
{{- $method := . }}
{{ if and .Request.IsDiscrete .Response.IsDiscrete }}
func (s *{{ $proxy }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)

    var err error
    {{- if .Request.Input }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
	inputBytes, err := proto.Marshal(input)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, err
	}
	{{- else }}
	var inputBytes []byte
	{{- end }}

	{{- if .Scope.IsPartition }}
	{{- if .Request.PartitionKey }}
	partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
	{{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
	partition := s.PartitionBy(partitionKey)
	{{- else }}
	partition := s.PartitionBy([]byte(partitionKey))
	{{- end }}
	{{- else if .Request.PartitionRange }}
	partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
	{{- else }}
	header := {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}
	partition := s.PartitionFor(header.PrimitiveID)
	{{- end }}

    {{- if .Response.Output }}
	outputBytes, err := partition.Do{{ template "optype" . }}(ctx, {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
	{{- else }}
	_, err = partition.Do{{ template "optype" . }}(ctx, {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, err
	}

	response := &{{ template "type" .Response.Type }}{}
	{{- if .Response.Output }}
    output := {{ template "ref" .Response.Output }}response{{ template "field" .Response.Output }}
	err = proto.Unmarshal(outputBytes, output)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, err
	}
	{{- end }}
	{{- else if .Scope.IsGlobal }}
	partitions := s.Partitions()
	{{- $outputs := false }}
	{{- if .Response.Output }}
    {{- range .Response.Output.Aggregates }}
    {{- $outputs = true }}
    {{- end }}
    {{- end }}

    {{- if $outputs }}
	outputsBytes, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
		return partitions[i].Do{{ template "optype" . }}(ctx, {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
	})
	{{- else }}
	err = async.IterAsync(len(partitions), func(i int) error {
		_, err := partitions[i].Do{{ template "optype" . }}(ctx, {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
		return err
	})
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, err
	}

    {{- if $outputs }}
	outputs := make([]{{ template "type" $method.Response.Output.Field.Type }}, 0, len(outputsBytes))
	for _, outputBytes := range outputsBytes {
	    output := {{ template "type" $method.Response.Output.Field.Type }}{}
        err = proto.Unmarshal(outputBytes.([]byte), {{ template "ref" $method.Response.Output }}output)
        if err != nil {
            s.log.Errorf("Request {{ $method.Request.Type.Name }} failed: %v", err)
            return nil, err
        }
        outputs = append(outputs, output)
	}
    {{- end }}

	response := &{{ template "type" .Response.Type }}{}
	{{- if .Response.Output }}
    {{- range .Response.Output.Aggregates }}
    {{- if .IsChooseFirst }}
    response{{ template "field" $method.Response.Output }}{{ template "field" . }} = outputs[0]{{ template "field" . }}
    {{- else if .IsAppend }}
    for _, o := range outputs {
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} = append(response{{ template "field" $method.Response.Output }}{{ template "field" . }}, o{{ template "field" . }}...)
    }
    {{- else if .IsSum }}
    for _, o := range outputs {
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} += o{{ template "field" . }}
    }
    {{- end }}
    {{- end }}
	{{- end }}
	{{- end }}
	s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
	return response, nil
}
{{ else if .Request.IsStream }}
func (s *{{ $proxy }}) {{ .Name }}(srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
	response := &{{ template "type" .Response.Type }}{}
    for {
        request, err := srv.Recv()
        if err == io.EOF {
            s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
            return srv.SendAndClose(response)
        } else if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }

        s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
        {{- if .Request.Input }}
        input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
        inputBytes, err := proto.Marshal(input)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }
        {{- else }}
        var inputBytes []byte
        {{- end }}

        {{- if .Scope.IsPartition }}
        {{- if .Request.PartitionKey }}
        partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
        {{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
        partition := s.PartitionBy(partitionKey)
        {{- else }}
        partition := s.PartitionBy([]byte(partitionKey))
        {{- end }}
        {{- else if .Request.PartitionRange }}
        partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
        {{- else }}
        header := {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}
        partition := s.PartitionFor(header.PrimitiveID)
        {{- end }}

        {{- if .Response.Output }}
        outputBytes, err := partition.Do{{ template "optype" . }}(srv.Context(), {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
        {{- else }}
        _, err = partition.Do{{ template "optype" . }}(srv.Context(), {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
        {{- end }}
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }

        {{- if .Response.Output }}
        output := {{ template "ref" .Response.Output }}response{{ template "field" .Response.Output }}
        err = proto.Unmarshal(outputBytes, output)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }
        {{- end }}
        {{- else if .Scope.IsGlobal }}
        partitions := s.Partitions()
        {{- $outputs := false }}
        {{- if .Response.Output }}
        {{- range .Response.Output.Aggregates }}
        {{- $outputs = true }}
        {{- end }}
        {{- end }}

        {{- if $outputs }}
        outputsBytes, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
            return partitions[i].Do{{ template "optype" . }}(srv.Context(), {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
        })
        {{- else }}
        err = async.IterAsync(len(partitions), func(i int) error {
            _, err := partitions[i].Do{{ template "optype" . }}(srv.Context(), {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }})
            return err
        })
        {{- end }}
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }

        {{- if $outputs }}
        outputs := make([]{{ template "type" $method.Response.Output.Field.Type }}, 0, len(outputsBytes))
        for _, outputBytes := range outputsBytes {
            output := {{ template "type" $method.Response.Output.Field.Type }}{}
            err = proto.Unmarshal(outputBytes.([]byte), {{ template "ref" $method.Response.Output }}output)
            if err != nil {
                s.log.Errorf("Request {{ $method.Request.Type.Name }} failed: %v", err)
                return err
            }
            outputs = append(outputs, output)
        }
        {{- end }}

        {{- if .Response.Output }}
        {{- range .Response.Output.Aggregates }}
        {{- if .IsChooseFirst }}
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} = outputs[0]{{ template "field" . }}
        {{- else if .IsAppend }}
        for _, o := range outputs {
            response{{ template "field" $method.Response.Output }}{{ template "field" . }} = append(response{{ template "field" $method.Response.Output }}{{ template "field" . }}, o{{ template "field" . }}...)
        }
        {{- else if .IsSum }}
        for _, o := range outputs {
            response{{ template "field" $method.Response.Output }}{{ template "field" . }} += o{{ template "field" . }}
        }
        {{- end }}
        {{- end }}
        {{- end }}
        {{- end }}
    }
}
{{ else if .Response.IsStream }}
func (s *{{ $proxy }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
    s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)

    var err error
    {{- if .Request.Input }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
	inputBytes, err := proto.Marshal(input)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
        return err
	}
	{{- else }}
	var inputBytes []byte
	{{- end }}

	stream := streams.NewBufferedStream()
	{{- if .Scope.IsPartition }}
	{{- if .Request.PartitionKey }}
	partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
	{{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
	partition := s.PartitionBy(partitionKey)
	{{- else }}
	partition := s.PartitionBy([]byte(partitionKey))
	{{- end }}
	{{- else if .Request.PartitionRange }}
	partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
	{{- else }}
	header := {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}
	partition := s.PartitionFor(header.PrimitiveID)
	{{- end }}

	err = partition.Do{{ template "optype" . }}Stream(srv.Context(), {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}, stream)
	{{- else if .Scope.IsGlobal }}
	partitions := s.Partitions()
	err = async.IterAsync(len(partitions), func(i int) error {
		return partitions[i].Do{{ template "optype" . }}Stream(srv.Context(), {{ $name }}, inputBytes, {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}, stream)
	})
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return err
	}

	for {
		result, ok := stream.Receive()
		if !ok {
			break
		}

		if result.Failed() {
			s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", result.Error)
			return result.Error
		}

		sessionOutput := result.Value.(rsm.SessionOutput)
		response := &{{ template "type" .Response.Type }}{
		    Header: sessionOutput.Header,
		}
		outputBytes := sessionOutput.Value.([]byte)
        output := {{ template "ref" .Response.Output }}response{{ template "field" .Response.Output }}
        err = proto.Unmarshal(outputBytes, output)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }

		s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
		if err = srv.Send(response); err != nil {
            s.log.Errorf("Response {{ .Response.Type.Name }} failed: %v", err)
			return err
		}
	}
	s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
	return nil
}
{{ end }}
{{- end }}
