{{- $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

package {{ .Package.Name }}

import (
	"github.com/atomix/go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
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

var new{{ $serviceInt }}Func rsm.NewServiceFunc

func register{{ $serviceInt }}Func(rsmf New{{ $serviceInt }}Func) {
	new{{ $serviceInt }}Func = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &{{ $serviceImpl }}{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(scheduler, context),
			log:     logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}, "service"),
		}
		service.init()
		return service
	}
}

type New{{ $serviceInt }}Func func(scheduler rsm.Scheduler, context rsm.ServiceContext) {{ $serviceInt }}

// Register{{ $serviceInt }} registers the election primitive service on the given node
func Register{{ $serviceInt }}(node *rsm.Node) {
	node.RegisterService({{ printf "%sType" .Generator.Prefix }}, new{{ $serviceInt }}Func)
}

type {{ $serviceImpl }} struct {
	rsm.Service
	rsm {{ $serviceInt }}
	log logging.Logger
}

func (s *{{ $serviceImpl }}) init() {
    {{- $root := . }}
    {{- range .Primitive.Methods }}
    {{- $name := ((printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel) }}
    {{- if and .Response.IsDiscrete (not .Type.IsAsync) }}
	s.RegisterUnaryOperation({{ $name }}, s.{{ .Name | toLowerCamel }})
    {{- else }}
	s.RegisterStreamOperation({{ $name }}, s.{{ .Name | toLowerCamel }})
    {{- end }}
    {{- end }}
}

func (s *{{ $serviceImpl }}) SessionOpen(session rsm.Session) {
    if sessionOpen, ok := s.rsm.(rsm.SessionOpenService); ok {
        sessionOpen.SessionOpen(session)
    }
}

func (s *{{ $serviceImpl }}) SessionExpired(session rsm.Session) {
    if sessionExpired, ok := s.rsm.(rsm.SessionExpiredService); ok {
        sessionExpired.SessionExpired(session)
    }
}

func (s *{{ $serviceImpl }}) SessionClosed(session rsm.Session) {
    if sessionClosed, ok := s.rsm.(rsm.SessionClosedService); ok {
        sessionClosed.SessionClosed(session)
    }
}

{{- range .Primitive.Methods }}
{{ if .Response.IsDiscrete }}
{{- if .Type.IsAsync }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(input []byte, stream rsm.Stream) (rsm.StreamCloser, error) {
    request := &{{ template "type" .Request.Type }}{}
    err := proto.Unmarshal(input, request)
    if err != nil {
        s.log.Error(err)
        return nil, err
    }
    future, err := s.rsm.{{ .Name }}(request)
    if err != nil {
        s.log.Error(err)
        return nil, err
    }
    future.setStream(stream)
    return nil, nil
}
{{- else }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(input []byte) ([]byte, error) {
    request := &{{ template "type" .Request.Type }}{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	response, err := s.rsm.{{ .Name }}(request)
	if err !=  nil {
	    s.log.Error(err)
    	return nil, err
	}

	output, err := proto.Marshal(response)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}
	return output, nil
}
{{- end }}
{{ else }}
{{- $newStream := printf "new%s%sStream" $serviceInt .Name }}
{{- $newInformer := printf "new%s%sInformer" $serviceInt .Name }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(input []byte, stream rsm.Stream) (rsm.StreamCloser, error) {
    request := &{{ template "type" .Request.Type }}{}
    err := proto.Unmarshal(input, request)
    if err != nil {
        s.log.Error(err)
        return nil, err
    }
    response := {{ $newStream }}(stream)
    closer, err := s.rsm.{{ .Name }}(request, response)
    if err != nil {
        s.log.Error(err)
        return nil, err
    }
    return closer, nil
}
{{ end }}
{{- end }}

var _ rsm.Service = &{{ $serviceImpl }}{}
