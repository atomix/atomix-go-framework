{{- $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

package {{ .Package.Name }}

import (
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- range .Primitive.Methods }}
	{{- if or .Type.IsAsync .Response.IsStream }}
	{{ import "github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm" }}
	{{ import "github.com/golang/protobuf/proto" }}
	{{- end }}
	{{- end }}
)

{{- $primitive := .Primitive }}
{{- range .Primitive.Methods }}
{{- if .Response.IsStream }}
{{- $streamInt := printf "%s%sStream" $serviceInt .Name }}
{{- $streamImpl := printf "%s%sStream" $serviceImpl .Name }}
{{- $newStream := printf "new%s%sStream" $serviceInt .Name }}
type {{ $streamInt }} interface {
	// ID returns the stream identifier
	ID() rsm.StreamID

	// OperationID returns the stream operation identifier
	OperationID() rsm.OperationID

	// Session returns the stream session
	Session() rsm.Session

	// Notify sends a value on the stream
	Notify(value *{{ template "type" .Response.Type }}) error

	// Close closes the stream
	Close()
}

func {{ $newStream }}(stream rsm.Stream) {{ $streamInt }} {
    return &{{ $streamImpl }}{
        stream: stream,
    }
}

type {{ $streamImpl }} struct {
    stream rsm.Stream
}

func (s *{{ $streamImpl }}) ID() rsm.StreamID {
    return s.stream.ID()
}

func (s *{{ $streamImpl }}) OperationID() rsm.OperationID {
    return s.stream.OperationID()
}

func (s *{{ $streamImpl }}) Session() rsm.Session {
    return s.stream.Session()
}

func (s *{{ $streamImpl }}) Notify(value *{{ template "type" .Response.Type }}) error {
    bytes, err := proto.Marshal(value)
    if err != nil {
        return err
    }
    s.stream.Value(bytes)
    return nil
}

func (s *{{ $streamImpl }}) Close() {
    s.stream.Close()
}

var _ {{ $streamInt }} = &{{ $streamImpl }}{}
{{- else if .Type.IsAsync }}
{{- $futureImpl := printf "%sFuture" .Response.Type.Name }}
type {{ $futureImpl }} struct {
    stream rsm.Stream
    output *{{ template "type" .Response.Type }}
    err error
}

func (f *{{ $futureImpl }}) setStream(stream rsm.Stream) {
    if f.output != nil {
        bytes, err := proto.Marshal(f.output)
        if err != nil {
            stream.Error(err)
        } else {
            stream.Value(bytes)
        }
        stream.Close()
    } else if f.err != nil {
        stream.Error(f.err)
        stream.Close()
    } else {
        f.stream = stream
    }
}

func (f *{{ $futureImpl }}) Complete(output *{{ template "type" .Response.Type }}) {
    if f.stream != nil {
        bytes, err := proto.Marshal(output)
        if err != nil {
            f.stream.Error(err)
        } else {
            f.stream.Value(bytes)
        }
        f.stream.Close()
    } else {
        f.output = output
    }
}

func (f *{{ $futureImpl }}) Fail(err error) {
    if f.stream != nil {
        f.stream.Error(err)
        f.stream.Close()
    } else {
        f.err = err
    }
}
{{- end }}
{{- end }}

type {{ $serviceInt }} interface {
    {{- range .Primitive.Methods }}

    {{- $comments := split .Comment "\n" }}
    {{- range $comment := $comments }}
    {{- if $comment }}
    // {{ $comment | trim }}
    {{- end }}
    {{- end }}

    {{- $streamInt := printf "%s%sStream" $serviceInt .Name }}
    {{- $informerInt := printf "%s%sInformer" $serviceInt .Name }}
    {{- $writerInt := printf "%s%sWriter" $serviceInt .Name }}

    {{- if .Response.IsDiscrete }}
    {{- if .Type.IsAsync }}
    {{- $futureImpl := printf "%sFuture" .Response.Type.Name }}
    {{ .Name }}(*{{ template "type" .Request.Type }}) (*{{ $futureImpl }}, error)
    {{- else }}
    {{ .Name }}(*{{ template "type" .Request.Type }}) (*{{ template "type" .Response.Type }}, error)
    {{- end }}
    {{- else if .Response.IsStream }}
    {{ .Name }}(*{{ template "type" .Request.Type }}, {{ $streamInt }}) (rsm.StreamCloser, error)
    {{- end }}
    {{- end }}
}
