{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}
{{- $server := printf "%sServer" .Generator.Prefix }}
{{- $service := printf "%sService" .Generator.Prefix }}
package {{ .Package.Name }}

import (
	"context"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
	"google.golang.org/grpc"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- range .Primitive.Methods }}
	{{- if .Request.IsStream }}
	{{ import "io" }}
	{{- end }}
	{{- if .Response.IsStream }}
	{{ import "sync" }}
	{{ import "github.com/atomix/go-framework/pkg/atomix/util/async" }}
	{{- end }}
	{{- end }}
)

// Register{{ $server }} registers the primitive on the given node
func Register{{ $server }}(node *gossip.Node) {
	node.RegisterServer(func(server *grpc.Server, manager *gossip.Manager) {
		{{ .Primitive.Type.Package.Alias }}.Register{{ .Primitive.Type.Name }}Server(server, new{{ $server }}(manager))
	})
}

func new{{ $server }}(manager *gossip.Manager) {{ .Primitive.Type.Package.Alias }}.{{ .Primitive.Type.Name }}Server {
	return &{{ $server }}{
		manager: manager,
		log: logging.GetLogger("atomix", "protocol", "gossip", {{ .Primitive.Name | lower | quote }}),
	}
}

{{- $primitive := .Primitive }}
{{- $serviceInt := printf "%sService" .Generator.Prefix }}
type {{ $server }} struct {
    manager *gossip.Manager
	log logging.Logger
}

{{- define "type" -}}
{{- if .Package.Import -}}
{{- printf "%s.%s" .Package.Alias .Name -}}
{{- else -}}
{{- .Name -}}
{{- end -}}
{{- end -}}

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
	s.manager.PrepareRequest({{ template "ref" .Request.Headers }}request{{ template "field" .Request.Headers }})
	{{- if .Scope.IsPartition }}
    partition, err := s.manager.PartitionFrom(ctx)
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return nil, err
    }

    service, err := partition.GetService(ctx, {{ $serviceType }}, gossip.ServiceID(request{{ template "field" .Request.Headers }}.PrimitiveID))
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }

    response, err := service.({{ $service }}).{{ .Name }}(ctx, request)
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return nil, errors.Proto(err)
    }
    s.manager.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
	{{- else if .Scope.IsGlobal }}
	partitions, err := s.manager.PartitionsFrom(ctx)
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
	    return nil, errors.Proto(err)
	}

	responses, err := async.ExecuteAsync(len(partitions), func(i int) (interface{}, error) {
	    partition := partitions[i]
	    service, err := partition.GetService(ctx, {{ $serviceType }}, gossip.ServiceID(request{{ template "field" .Request.Headers }}.PrimitiveID))
	    if err != nil {
	        return nil, err
	    }
	    response, err := service.({{ $service }}).{{ .Name }}(ctx, request)
	    if err != nil {
	        return nil, err
	    }
    	s.manager.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
    	return response, nil
    })
	if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
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
func (s *{{ $server }}) {{ .Name }}(request *{{ template "type" .Request.Type }}, srv {{ template "type" $primitive.Type }}_{{ .Name }}Server) error {
    s.log.Debugf("Received {{ .Request.Type.Name }} %+v", request)
	s.manager.PrepareRequest({{ template "ref" .Request.Headers }}request{{ template "field" .Request.Headers }})

    partitions, err := s.manager.PartitionsFrom(srv.Context())
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    responseCh := make(chan {{ template "type" .Response.Type }})
    wg := &sync.WaitGroup{}
    wg.Add(len(partitions))
    err = async.IterAsync(len(partitions), func(i int) error {
        partition := partitions[i]
        service, err := partition.GetService(srv.Context(), {{ $serviceType }}, gossip.ServiceID(request{{ template "field" .Request.Headers }}.PrimitiveID))
        if err != nil {
            return err
        }

        partitionCh := make(chan {{ template "type" .Response.Type }})
		errCh := make(chan error)
		go func() {
            err := service.({{ $service }}).{{ .Name }}(srv.Context(), request, partitionCh)
			if err != nil {
				errCh <- err
			}
			close(errCh)
		}()

		go func() {
			defer wg.Done()
			for response := range partitionCh {
				responseCh <- response
			}
		}()
		return <-errCh
    })
    if err != nil {
        s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
        return errors.Proto(err)
    }

    go func() {
        wg.Wait()
        close(responseCh)
    }()

    for {
        select {
        case response, ok := <-responseCh:
            if ok {
                s.manager.PrepareResponse({{ template "ref" .Response.Headers }}response{{ template "field" .Response.Headers }})
                s.log.Debugf("Sending {{ .Response.Type.Name }} %v", response)
                err = srv.Send(&response)
                if err != nil {
                    s.log.Errorf("Request {{ .Request.Type.Name }} %+v failed: %v", request, err)
                    return errors.Proto(err)
                }
            } else {
                s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
                return nil
            }
        case <-srv.Context().Done():
            s.log.Debugf("Finished {{ .Request.Type.Name }} %+v", request)
            return nil
        }
    }
}
{{ end }}
{{- end }}
