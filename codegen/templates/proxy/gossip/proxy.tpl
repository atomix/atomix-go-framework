{{- $proxy := printf "%sProxyServer" .Primitive.Name }}
{{- $service := printf "%s.%sServer" .Primitive.Type.Package.Alias .Primitive.Type.Name }}
package {{ .Package.Name }}

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/gossip"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- range .Primitive.Methods }}
	{{- if .Scope.IsGlobal }}
	{{ import "github.com/atomix/go-framework/pkg/atomix/util/async" }}
	{{- end }}
	{{- if or .Request.IsStream .Response.IsStream }}
	{{ import "io" }}
	{{- end }}
	{{- if and .Scope.IsGlobal (or .Request.IsStream .Response.IsStream) }}
	{{ import "sync" }}
	{{- end }}
	{{- end }}
)

{{ $root := . }}

// New{{ $proxy }} creates a new {{ $proxy }}
func New{{ $proxy }}(client *gossip.Client) {{ $service }} {
	return &{{ $proxy }}{
        Proxy: gossip.NewProxy(client),
        log:   logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}),
    }
}

{{- $primitive := .Primitive }}
type {{ $proxy }} struct {
	*gossip.Proxy
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
func (s *{{ $proxy }}) {{ .Name }}(ctx context.Context, request *{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error) {
	s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
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

	conn, err := partition.Connect()
	if err != nil {
		return nil, errors.Proto(err)
	}

	client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
	s.PrepareRequest({{ template "ref" .Request.Headers }}request{{ template "field" .Request.Headers }})
	ctx = partition.AddHeaders(ctx)
	response, err := client.{{ .Name }}(ctx, request)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, errors.Proto(err)
	}
	s.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
	{{- else if .Scope.IsGlobal }}
	partitions := s.Partitions()
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
        partition := partitions[i]
        conn, err := partition.Connect()
        if err != nil {
            return nil, err
        }
        client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
    	s.PrepareRequest({{ template "ref" .Request.Headers }}request{{ template "field" .Request.Headers }})
        ctx := partition.AddHeaders(ctx)
		response, err := client.{{ .Name }}(ctx, request)
		if err != nil {
		    return nil, err
		}
    	s.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
    	return response, nil
	})
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, errors.Proto(err)
	}

	response := responses[0].(*{{ template "type" .Response.Type }})
    {{- range .Response.Aggregates }}
    {{- if .IsChooseFirst }}
    response{{ template "field" . }} = responses[0].(*{{ template "type" $method.Response.Type }}){{ template "field" . }}
    {{- else if .IsAppend }}
    for _, r := range responses {
        response{{ template "field" . }} = append(response{{ template "field" . }}, r.(*{{ template "type" $method.Response.Type }}){{ template "field" . }}...)
    }
    {{- else if .IsSum }}
    for _, r := range responses {
        response{{ template "field" . }} += r.(*{{ template "type" $method.Response.Type }}){{ template "field" . }}
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
	{{- if .Scope.IsPartition }}
	{{- if .Request.PartitionKey }}
	partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
	{{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
	partition := s.PartitionBy(partitionKey)
	{{- else }}
	{{- if .Request.PartitionKey.Type.IsString }}
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

	conn, err := partition.Connect()
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
		return errors.Proto(err)
	}

	client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
	s.PrepareRequest({{ template "ref" .Request.Headers }}request{{ template "field" .Request.Headers }})
	ctx := partition.AddHeaders(srv.Context())
	stream, err := client.{{ .Name }}(ctx, request)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
		return errors.Proto(err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
			return nil
		} else if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
			return errors.Proto(err)
		}
    	s.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
		s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
		if err := srv.Send(response); err != nil {
            s.log.Errorf("Response {{ .Response.Type.Name }} failed: %v", err)
			return err
		}
	}
	{{- else if .Scope.IsGlobal }}
	partitions := s.Partitions()
    wg := &sync.WaitGroup{}
    responseCh := make(chan *{{ template "type" .Response.Type }})
    errCh := make(chan error)
    err := async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        conn, err := partition.Connect()
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }
        client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
	    s.PrepareRequest({{ template "ref" .Request.Headers }}request{{ template "field" .Request.Headers }})
        ctx := partition.AddHeaders(srv.Context())
        stream, err := client.{{ .Name }}(ctx, request)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                response, err := stream.Recv()
                if err == io.EOF {
                    return
                } else if err != nil {
                    errCh <- err
                } else {
                    responseCh <- response
                }
            }
        }()
        return nil
    })
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
        return errors.Proto(err)
    }

    go func() {
        wg.Wait()
        close(responseCh)
        close(errCh)
    }()

    for {
        select {
        case response, ok := <-responseCh:
            if ok {
    	        s.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
                s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
                err := srv.Send(response)
                if err != nil {
                    s.log.Errorf("Response {{ .Response.Type.Name }} failed: %v", err)
                    return err
                }
            }
        case err := <-errCh:
            if err != nil {
                s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            }
			s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
            return err
        }
    }
	{{- end }}
}
{{ end }}
{{- end }}
