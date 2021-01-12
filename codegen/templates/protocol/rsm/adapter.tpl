{{- $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

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

package {{ .Package.Name }}

import (
	"github.com/atomix/go-framework/pkg/atomix/protocol/rsm"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	{{- $added := false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) (or .Type.IsSnapshot .Type.IsRestore) }}
	"github.com/atomix/go-framework/pkg/atomix/util"
	"io"
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

{{ range .Primitive.Methods }}
{{- if .Type.IsSnapshot }}
func (s *{{ $serviceImpl }}) Backup(writer io.Writer) error {
    {{- if .Response.IsDiscrete }}
    snapshot, err := s.rsm.{{ .Name }}()
    if err != nil {
	    s.log.Error(err)
        return err
    }
    bytes, err := proto.Marshal(snapshot)
    if err != nil {
	    s.log.Error(err)
        return err
    }
    return util.WriteBytes(writer, bytes)
    {{- else if .Response.IsStream }}
    {{- $newWriter := printf "new%s%sWriter" $serviceInt .Name }}
    err := s.rsm.{{ .Name }}({{ $newWriter }}(writer))
    if err != nil {
	    s.log.Error(err)
	    return err
    }
    return nil
    {{- end }}
}
{{- end }}
{{ end }}

{{- range .Primitive.Methods }}
{{- if .Type.IsRestore }}
func (s *{{ $serviceImpl }}) Restore(reader io.Reader) error {
    {{- if .Request.IsDiscrete }}
    bytes, err := util.ReadBytes(reader)
    if err != nil {
	    s.log.Error(err)
        return err
    }
    snapshot := &{{ template "type" .Request.Input.Field.Type }}{}
    err = proto.Unmarshal(bytes, snapshot)
    if err != nil {
        return err
    }
    err = s.rsm.{{ .Name }}(snapshot)
    if err != nil {
	    s.log.Error(err)
	    return err
    }
    return nil
    {{- else if .Request.IsStream }}
    for {
        bytes, err := util.ReadBytes(reader)
        if err == io.EOF {
            return nil
        } else if err != nil {
            s.log.Error(err)
            return err
        }

        entry := &{{ template "type" .Request.Input.Field.Type }}{}
        err = proto.Unmarshal(bytes, entry)
        if err != nil {
            s.log.Error(err)
            return err
        }
        err = s.rsm.{{ .Name }}(entry)
        if err != nil {
            s.log.Error(err)
            return err
        }
    }
    {{- end }}
}
{{- end }}
{{- end }}

{{- range .Primitive.Methods }}
{{ if .Response.IsDiscrete }}
{{- if .Type.IsAsync }}
{{- $newStream := printf "new%s%sStream" $serviceInt .Name }}
{{- $newInformer := printf "new%s%sInformer" $serviceInt .Name }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(in []byte, stream rsm.Stream) {
{{- if and .Request.Input .Response.Output }}
    input := &{{ template "type" .Request.Input.Field.Type }}{}
    err := proto.Unmarshal(in, input)
    if err != nil {
        s.log.Error(err)
        stream.Error(err)
        stream.Close()
        return
    }
    future, err := s.rsm.{{ .Name }}(input)
{{- else if .Request.Input }}
    input := &{{ template "type" .Request.Input.Field.Type }}{}
    err := proto.Unmarshal(in, input)
    if err != nil {
        s.log.Error(err)
        stream.Error(err)
        stream.Close()
        return
    }
    future, err := s.rsm.{{ .Name }}(input)
{{- else }}
    future, err := s.rsm.{{ .Name }}()
{{- end }}
    if err != nil {
        s.log.Error(err)
        stream.Error(err)
        stream.Close()
        return
    }
    future.setStream(stream)
}
{{- else }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(in []byte) ([]byte, error) {
{{- if and .Request.Input .Response.Output }}
    input := &{{ template "type" .Request.Input.Field.Type }}{}
	err := proto.Unmarshal(in, input)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}

	output, err := s.rsm.{{ .Name }}(input)
	if err !=  nil {
	    s.log.Error(err)
    	return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}
	return out, nil
{{- else if .Request.Input }}
    input := &{{ template "type" .Request.Input.Field.Type }}{}
	err := proto.Unmarshal(in, input)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}
	err = s.rsm.{{ .Name }}(input)
	if err != nil {
	    s.log.Error(err)
	    return nil, err
	}
	return nil, nil
{{- else if .Response.Output }}
	output, err := s.rsm.{{ .Name }}()
	if err !=  nil {
	    s.log.Error(err)
    	return nil, err
	}

	out, err := proto.Marshal(output)
	if err != nil {
	    s.log.Error(err)
		return nil, err
	}
	return out, nil
{{- else }}
	err := s.rsm.{{ .Name }}()
	if err != nil {
	    s.log.Error(err)
	    return nil, err
	}
	return nil, nil
{{- end }}
}
{{- end }}
{{ else }}
{{- $newStream := printf "new%s%sStream" $serviceInt .Name }}
{{- $newInformer := printf "new%s%sInformer" $serviceInt .Name }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(in []byte, stream rsm.Stream) {
{{- if .Type.IsSnapshot }}
    {{- $newWriter := printf "new%s%sStreamWriter" $serviceInt .Name }}
    err := s.rsm.{{ .Name }}({{ $newWriter }}(stream))
{{- else if and .Request.Input .Response.Output }}
    input := &{{ template "type" .Request.Input.Field.Type }}{}
    err := proto.Unmarshal(in, input)
    if err != nil {
        s.log.Error(err)
        stream.Error(err)
        stream.Close()
        return
    }
    output := {{ $newStream }}(stream)
    err = s.rsm.{{ .Name }}(input, output)
{{- else if .Request.Input }}
    input := &{{ template "type" .Request.Input.Field.Type }}{}
    err := proto.Unmarshal(in, input)
    if err != nil {
        s.log.Error(err)
        stream.Error(err)
        stream.Close()
        return
    }
    output := {{ $newInformer }}(stream)
    err = s.rsm.{{ .Name }}(input, output)
{{- else if .Response.Output }}
    output := {{ $newStream }}(stream)
    err := s.rsm.{{ .Name }}(output)
{{- else }}
    output := {{ $newInformer }}(stream)
    err := s.rsm.{{ .Name }}(output)
{{- end }}
    if err != nil {
        s.log.Error(err)
        stream.Error(err)
        stream.Close()
        return
    }
}
{{ end }}
{{- end }}

var _ rsm.Service = &{{ $serviceImpl }}{}
