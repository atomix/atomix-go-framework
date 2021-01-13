{{- $proxy := printf "%sProxy" .Generator.Prefix }}
package {{ .Package.Name }}

import (
    "fmt"
	"context"
	"github.com/atomix/go-framework/pkg/atomix/proxy/p2p"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	{{- $added := false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Scope.IsGlobal }}
	"github.com/atomix/go-framework/pkg/atomix/util/async"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $added = false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) (or .Request.IsStream .Response.IsStream) }}
	"io"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $added = false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Scope.IsGlobal (or .Request.IsStream .Response.IsStream) }}
	"sync"
	{{- $added = true }}
	{{- end }}
	{{- end }}
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
func Register{{ $proxy }}(node *p2p.Node) {
	node.RegisterProxy({{ .Primitive.Name | quote }}, func(server *grpc.Server, client *p2p.Client) {
		{{ .Primitive.Type.Package.Alias }}.Register{{ .Primitive.Type.Name }}Server(server, &{{ $proxy }}{
			Proxy: p2p.NewProxy(client),
			log: logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}),
		})
	})
}

{{- $primitive := .Primitive }}
type {{ $proxy }} struct {
	*p2p.Proxy
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
	partition := s.PartitionBy([]byte(partitionKey))
	{{- end }}
	{{- else if .Request.PartitionRange }}
	partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
	{{- else }}
	header := {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}
	partition := s.PartitionFor(header.PrimitiveID)
	{{- end }}

	conn, err := partition.Connect()
	if err != nil {
		return nil, err
	}

	client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
	ctx = partition.AddHeader(ctx)
	response, err := client.{{ .Name }}(ctx, request)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, err
	}
	{{- else if .Scope.IsGlobal }}
	partitions := s.Partitions()
	{{- $outputs := false }}
	{{- if .Response.Output }}
    {{- range .Response.Output.Aggregates }}
    {{- $outputs = true }}
    {{- end }}
    {{- end }}
    {{- if $outputs }}
	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
        partition := partitions[i]
        conn, err := partition.Connect()
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return nil, err
        }
        client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
        ctx = partition.AddHeader(ctx)
		return client.{{ .Name }}(ctx, request)
	})
	{{- else }}
	err := async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        conn, err := partition.Connect()
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }
        client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
        ctx = partition.AddHeader(ctx)
		_, err = client.{{ .Name }}(ctx, request)
		return err
	})
	{{- end }}
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
	    return nil, err
	}

	response := &{{ template "type" .Response.Type }}{}
    {{- if .Response.Output }}
    {{- range .Response.Output.Aggregates }}
    {{- if .IsChooseFirst }}
    response{{ template "field" $method.Response.Output }}{{ template "field" . }} = responses[0].(*{{ template "type" $method.Response.Type }}){{ template "field" $method.Response.Output }}{{ template "field" . }}
    {{- else if .IsAppend }}
    for _, r := range responses {
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} = append(response{{ template "field" $method.Response.Output }}{{ template "field" . }}, r.(*{{ template "type" $method.Response.Type }}){{ template "field" $method.Response.Output }}{{ template "field" . }}...)
    }
    {{- else if .IsSum }}
    for _, r := range responses {
        response{{ template "field" $method.Response.Output }}{{ template "field" . }} += r.(*{{ template "type" $method.Response.Type }}){{ template "field" $method.Response.Output }}{{ template "field" . }}
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
    {{- if and .Scope.IsPartition (not .Request.PartitionKey) (not .Request.PartitionRange) }}
    var stream {{ template "type" $primitive.Type }}_{{ .Name }}Client
    {{- else if .Scope.IsGlobal }}
    var streams map[p2p.PartitionID]{{ template "type" $primitive.Type }}_{{ .Name }}Client
    {{- end }}
    for {
        request, err := srv.Recv()
        if err == io.EOF {
            {{- if and .Scope.IsPartition (not .Request.PartitionKey) (not .Request.PartitionRange) }}
            if stream == nil {
                return nil
            }

            response, err := stream.CloseAndRecv()
            if err != nil {
                s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
                return err
            }
            {{- else }}
            if streams == nil {
                return nil
            }

            responses := make([]*{{ template "type" $method.Response.Type }}, 0, len(streams))
            for _, stream := range streams {
                response, err := stream.CloseAndRecv()
                if err != nil {
                    s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
                    return err
                }
                responses = append(responses, response)
            }
            {{- $outputs := false }}
            {{- if .Response.Output }}
            {{- range .Response.Output.Aggregates }}
            {{- $outputs = true }}
            {{- end }}
            {{- end }}
            clients := make([]{{ template "type" $primitive.Type }}_{{ .Name }}Client, 0, len(streams))
            for _, stream := range streams {
                clients = append(clients, stream)
            }
            {{- if $outputs }}
            responses, err := async.ExecuteAsync(len(clients), func(i int) (interface{}, error) {
                return clients[i].CloseAndRecv()
            })
            {{- else }}
            err := async.IterAsync(len(clients), func(i int) error {
                _, err = clients[i].CloseAndRecv()
                return err
            })
            {{- end }}
            if err != nil {
                s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
                return err
            }

            response := &{{ template "type" .Response.Type }}{}
            {{- if .Response.Output }}
            {{- range .Response.Output.Aggregates }}
            {{- if .IsChooseFirst }}
            response{{ template "field" $method.Response.Output }}{{ template "field" . }} = responses[0].(*{{ template "type" $method.Response.Type }}){{ template "field" $method.Response.Output }}{{ template "field" . }}
            {{- else if .IsAppend }}
            for _, r := range responses {
                response{{ template "field" $method.Response.Output }}{{ template "field" . }} = append(response{{ template "field" $method.Response.Output }}{{ template "field" . }}, r.(*{{ template "type" $method.Response.Type }}){{ template "field" $method.Response.Output }}{{ template "field" . }}...)
            }
            {{- else if .IsSum }}
            for _, r := range responses {
                response{{ template "field" $method.Response.Output }}{{ template "field" . }} += r.(*{{ template "type" $method.Response.Type }}){{ template "field" $method.Response.Output }}{{ template "field" . }}
            }
            {{- end }}
            {{- end }}
            {{- end }}
            {{- end }}
            s.log.Debugf("Sending {{ .Response.Type.Name }} %+v", response)
            return srv.SendAndClose(response)
        } else if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }

        s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
        {{- if and .Scope.IsPartition (not .Request.PartitionKey) (not .Request.PartitionRange) }}
        if stream == nil {
            header := {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}
            partition := s.PartitionFor(header.PrimitiveID)
            conn, err := partition.Connect()
            if err != nil {
                return err
            }
            client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
            ctx := partition.AddHeader(srv.Context())
            stream, err = client.{{ .Name }}(ctx)
            if err != nil {
                return err
            }
        }
        {{- else }}
        if streams == nil {
            partitions := s.Partitions()
            streams = make(map[p2p.PartitionID]{{ template "type" $primitive.Type }}_{{ .Name }}Client)
            for _, partition := range partitions {
                conn, err := partition.Connect()
                if err != nil {
                    return err
                }
                client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
                ctx := partition.AddHeader(srv.Context())
                stream, err := client.{{ .Name }}(ctx)
                if err != nil {
                    return err
                }
                streams[partition.ID] = stream
            }
        }
        {{- end }}

        {{- if .Scope.IsPartition }}
        {{- if .Request.PartitionKey }}
        partitionKey := {{ template "val" .Request.PartitionKey }}request{{ template "field" .Request.PartitionKey }}
        {{- if and .Request.PartitionKey.Field.Type.IsBytes (not .Request.PartitionKey.Field.Type.IsCast) }}
        partition := streams[s.PartitionBy(partitionKey).ID]
        {{- else }}
        partition := streams[s.PartitionBy([]byte(partitionKey)).ID]
        {{- end }}
        {{- else if .Request.PartitionRange }}
        partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
        partition := streams[s.PartitionByRange(partitionRange).ID]
        {{- else }}
        partition := stream
        {{- end }}

        err = partition.Send(request)
        if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
            return err
        }
        {{- else if .Scope.IsGlobal }}
        for _, stream := range streams {
            err := stream.Send(request)
            if err != nil {
                s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
                return err
            }
        }
        {{- end }}
    }
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
	partition := s.PartitionBy([]byte(partitionKey))
	{{- end }}
	{{- else if .Request.PartitionRange }}
	partitionRange := {{ template "val" .Request.PartitionRange }}request{{ template "field" .Request.PartitionRange }}
	{{- else }}
	header := {{ template "val" .Request.Header }}request{{ template "field" .Request.Header }}
	partition := s.PartitionFor(header.PrimitiveID)
	{{- end }}

	conn, err := partition.Connect()
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
		return err
	}

	client := {{ $primitive.Type.Package.Alias }}.New{{ $primitive.Type.Name }}Client(conn)
	ctx := partition.AddHeader(srv.Context())
	stream, err := client.{{ .Name }}(ctx, request)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
		return err
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
			return nil
		} else if err != nil {
            s.log.Errorf("Request {{ .Request.Type.Name }} failed: %v", err)
			return err
		}
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
        ctx := partition.AddHeader(srv.Context())
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
        return err
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
