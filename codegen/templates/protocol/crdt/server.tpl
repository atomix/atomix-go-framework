{{- $server := printf "%sServer" .Generator.Prefix }}
package {{ .Package.Name }}

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/crdt"
	"google.golang.org/grpc"
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
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
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
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

// Register{{ $server }} registers the primitive on the given node
func Register{{ $server }}(node *crdt.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *crdt.Manager) {
		{{ .Primitive.Type.Package.Alias }}.Register{{ .Primitive.Type.Name }}Server(server, new{{ $server }}(newManager(manager)))
	})
	node.RegisterServer(registerServerFunc)
}

var registerServerFunc crdt.RegisterServerFunc

func new{{ $server }}(manager *Manager) {{ .Primitive.Type.Package.Alias }}.{{ .Primitive.Type.Name }}Server {
	return &{{ $server }}{
		manager: manager,
		log: logging.GetLogger("atomix", "protocol", "crdt", {{ .Primitive.Name | lower | quote }}),
	}
}

{{- $primitive := .Primitive }}
{{- $serviceInt := printf "%sService" .Generator.Prefix }}
type {{ $server }} struct {
    manager *Manager
	log logging.Logger
}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

{{- define "cast" }}.(*{{ template "type" .Field.Type }}){{ end }}

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

{{- range .Primitive.Methods }}
{{- $method := . }}
{{ if and .Request.IsDiscrete .Response.IsDiscrete }}
func (s *{{ $server }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
	{{- if .Scope.IsPartition }}
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return nil, err
    }

    service, err := partition.GetService(request.Header.PrimitiveID.Name)
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    {{- if and .Request.Input .Response.Output }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
    output, err := service.{{ .Name }}(ctx, input)
    {{- else if .Request.Input }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
    err = service.{{ .Name }}(ctx, input)
    {{- else if .Response.Output }}
    output, err := service.{{ .Name }}(ctx)
    {{- else }}
    err = service.{{ .Name }}(ctx)
    {{- end }}
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response := &{{ template "type" .Response.Type }}{}
    {{- if .Response.Output }}
    {{- if .Response.Output.Field.Type.IsPointer }}
    response{{ template "field" .Response.Output }} = output
    {{- else }}
    response{{ template "field" .Response.Output }} = *output
    {{- end }}
    {{- end }}
	{{- else if .Scope.IsGlobal }}
	partitions, err := s.manager.PartitionsFrom(ctx)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

	{{- if .Request.Input }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
	{{- end }}

	{{- if .Response.Output }}
	outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
	    partition := partitions[i]
	    service, err := partition.GetService(request.Header.PrimitiveID.Name)
	    if err != nil {
	        return nil, err
	    }
	    {{- if .Request.Input }}
	    return service.{{ .Name }}(ctx, input)
	    {{- else }}
	    return service.{{ .Name }}(ctx)
	    {{- end }}
    })
    {{- else }}
    err = async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        service, err := partition.GetService(request.Header.PrimitiveID.Name)
        if err != nil {
            return err
        }
	    {{- if .Request.Input }}
	    return service.{{ .Name }}(ctx, input)
	    {{- else }}
	    return service.{{ .Name }}(ctx)
	    {{- end }}
    })
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

    response := &{{ template "type" .Response.Type }}{}
    {{- if .Response.Output }}
    {{- range .Response.Output.Aggregates }}
    {{- if .IsChooseFirst }}
    response{{ template "field" $method.Response.Output }}{{ template "field" . }} = outputs[0]{{ template "cast" $method.Response.Output }}{{ template "field" . }}
    {{- else if .IsAppend }}
    for _, o := range outputs {
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} = append(response{{ template "field" $method.Response.Output }}{{ template "field" . }}, o{{ template "cast" $method.Response.Output }}{{ template "field" . }}...)
    }
    {{- else if .IsSum }}
    for _, o := range outputs {
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} += o{{ template "cast" $method.Response.Output }}{{ template "field" . }}
    }
    {{- end }}
    {{- end }}
    {{- end }}
	{{- end }}
	s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
	return response, nil
}
{{ else if .Request.IsStream }}
func (s *{{ $server }}) {{ .Name }}(srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
    response := &{{ template "type" .Response.Type }}{}
    for {
        request, err := srv.Recv()
        if err == io.EOF {
            s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
            return srv.SendAndClose(response)
        } else if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return errors.Proto(err)
        }

        s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
        {{- if .Scope.IsPartition }}
        partition, err := s.manager.PartitionFrom(srv.Context())
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return err
        }

        service, err := partition.GetService(request.Header.PrimitiveID.Name)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return errors.Proto(err)
        }

        {{- if and .Request.Input .Response.Output }}
        input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
        output, err := service.{{ .Name }}(srv.Context(), input)
        {{- else if .Request.Input }}
        input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
        err = service.{{ .Name }}(srv.Context(), input)
        {{- else if .Response.Output }}
        output, err := service.{{ .Name }}(srv.Context())
        {{- else }}
        err = service.{{ .Name }}(srv.Context())
        {{- end }}
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return errors.Proto(err)
        }

        response = &{{ template "type" .Response.Type }}{}
        {{- if .Response.Output }}
        {{- if .Response.Output.Field.Type.IsPointer }}
        response{{ template "field" .Response.Output }} = output
        {{- else }}
        response{{ template "field" .Response.Output }} = *output
        {{- end }}
        {{- end }}
        {{- else if .Scope.IsGlobal }}
        partitions, err := s.manager.PartitionsFrom(srv.Context())
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return errors.Proto(err)
        }

        {{- if .Request.Input }}
        input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
        {{- end }}

        {{- if .Response.Output }}
        outputs, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
            partition := partitions[i]
            service, err := partition.GetService(request.Header.PrimitiveID.Name)
            if err != nil {
                return nil, err
            }
            {{- if .Request.Input }}
            return service.{{ .Name }}(srv.Context(), input)
            {{- else }}
            return service.{{ .Name }}(srv.Context())
            {{- end }}
        })
        {{- else }}
        err = async.IterAsync(len(partitions), func(i int) error {
            partition := partitions[i]
            service, err := partition.GetService(request.Header.PrimitiveID.Name)
            if err != nil {
                return err
            }
            {{- if .Request.Input }}
            return service.{{ .Name }}(srv.Context(), input)
            {{- else }}
            return service.{{ .Name }}(srv.Context())
            {{- end }}
        })
        {{- end }}
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return errors.Proto(err)
        }

        response = &{{ template "type" .Response.Type }}{}
        {{- if .Response.Output }}
        {{- range .Response.Output.Aggregates }}
        {{- if .IsChooseFirst }}
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} = outputs[0]{{ template "cast" $method.Response.Output }}{{ template "field" . }}
        {{- else if .IsAppend }}
        for _, o := range outputs {
            response{{ template "field" $method.Response.Output }}{{ template "field" . }} = append(response{{ template "field" $method.Response.Output }}{{ template "field" . }}, o{{ template "cast" $method.Response.Output }}{{ template "field" . }}...)
        }
        {{- else if .IsSum }}
        for _, o := range outputs {
            response{{ template "field" $method.Response.Output }}{{ template "field" . }} += o{{ template "cast" $method.Response.Output }}{{ template "field" . }}
        }
        {{- end }}
        {{- end }}
        {{- end }}
        {{- end }}
    }
}
{{ else if .Response.IsStream }}
{{- $newStream := printf "new%s%sStream" $serviceInt .Name }}
{{- $newInformer := printf "new%s%sInformer" $serviceInt .Name }}
func (s *{{ $server }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
    s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)

    stream := streams.NewBufferedStream()
    {{- if .Scope.IsPartition }}
    partition, err := s.manager.PartitionFrom(srv.Context())
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    service, err := partition.GetService(request.Header.PrimitiveID.Name)
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    {{- if .Type.IsSnapshot }}
    {{- $newWriter := printf "new%s%sStreamWriter" $serviceInt .Name }}
    err = service.{{ .Name }}(srv.Context(), {{ $newWriter }}(stream))
    {{- else if and .Request.Input .Response.Output }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
    err = service.{{ .Name }}(srv.Context(), input, {{ $newStream }}(stream))
    {{- else if .Request.Input }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
    err = service.{{ .Name }}(srv.Context(), input, {{ $newInformer }}(stream))
    {{- else if .Response.Output }}
    err = service.{{ .Name }}(srv.Context(), {{ $newStream }}(stream))
    {{- else }}
    err = service.{{ .Name }}(srv.Context(), {{ $newInformer }}(stream))
    {{- end }}
    {{- else if .Scope.IsGlobal }}
    partitions, err := s.manager.PartitionsFrom(srv.Context())
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    {{- if .Request.Input }}
    input := {{ template "ref" .Request.Input }}request{{ template "field" .Request.Input }}
    {{- end }}
    err = async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        service, err := partition.GetService(request.Header.PrimitiveID.Name)
        if err != nil {
            return err
        }
        {{- if .Type.IsSnapshot }}
        {{- $newWriter := printf "new%s%sStreamWriter" $serviceInt .Name }}
        return service.{{ .Name }}(srv.Context(), {{ $newWriter }}(stream))
        {{- else if and .Request.Input .Response.Output }}
        return service.{{ .Name }}(srv.Context(), input, {{ $newStream }}(stream))
        {{- else if .Request.Input }}
        return service.{{ .Name }}(srv.Context(), input, {{ $newInformer }}(stream))
        {{- else if .Response.Output }}
        return service.{{ .Name }}(srv.Context(), {{ $newStream }}(stream))
        {{- else }}
        return service.{{ .Name }}(srv.Context(), {{ $newInformer }}(stream))
        {{- end }}
    })
    {{- end }}
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    response := &{{ template "type" .Response.Type }}{}
    response{{ template "field" .Response.Header }} = primitiveapi.ResponseHeader{
        ResponseType: primitiveapi.ResponseType_RESPONSE_STREAM,
    }
    err = srv.Send(response)
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    for {
        result, ok := stream.Receive()
        if !ok {
            break
        }

        if result.Failed() {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, result.Error)
            return errors.Proto(result.Error)
        }

        response := &{{ template "type" .Response.Type }}{}
        {{- if .Response.Output }}
        {{- if .Response.Output.Field.Type.IsPointer }}
        response{{ template "field" .Response.Output }} = result.Value{{ template "cast" .Response.Output }}
        {{- else }}
        response{{ template "field" .Response.Output }} = *result.Value{{ template "cast" .Response.Output }}
        {{- end }}
        {{- end }}

        err = srv.Send(response)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
            return errors.Proto(err)
        }
    }

    s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
    return nil
}
{{ end }}
{{- end }}
