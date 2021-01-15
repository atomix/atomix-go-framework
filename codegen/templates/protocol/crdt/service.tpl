{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}
{{- $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

{{- define "type" }}{{ printf "%s.%s" .Package.Alias .Name }}{{ end }}

package {{ .Package.Name }}

import (
	"context"
	{{- $added := false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Type.IsSnapshot .Response.IsStream }}
	"github.com/atomix/go-framework/pkg/atomix/util"
	"io"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $added = false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) (not .Type.IsSnapshot) (or .Type.IsAsync .Response.IsStream) }}
	"github.com/golang/protobuf/proto"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $added = false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) (or .Type.IsAsync .Response.IsStream) }}
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	"github.com/atomix/go-framework/pkg/atomix/protocol/crdt"
)

const {{ $serviceType }} crdt.ServiceType = {{ .Primitive.Name | quote }}

// Register{{ $serviceInt }} registers the service on the given node
func Register{{ $serviceInt }}(node *crdt.Node) {
    node.RegisterService({{ $serviceType }}, newServiceFunc)
}

var newServiceFunc crdt.NewServiceFunc

{{- $primitive := .Primitive }}
{{- range .Primitive.Methods }}
{{- if .Response.IsStream }}
{{- if .Response.Output }}
{{- if .Type.IsSnapshot }}
{{- $output := include "type" .Response.Output.Field.Type }}
{{- $writerInt := printf "%s%sWriter" $serviceInt .Name }}
{{- $writerImpl := printf "%s%sWriter" $serviceImpl .Name }}
{{- $newWriter := printf "new%s%sWriter" $serviceInt .Name }}
type {{ $writerInt }} interface {
	// Write writes a value to the stream
	Write(value *{{ $output }}) error

	// Close closes the stream
	Close()
}

func {{ $newWriter }}(writer io.Writer) {{ $writerInt }} {
    return &{{ $writerImpl }}{
        writer: writer,
    }
}

type {{ $writerImpl }} struct {
    writer io.Writer
}

func (s *{{ $writerImpl }}) Write(value *{{ $output }}) error {
    bytes, err := proto.Marshal(value)
    if err != nil {
        return err
    }
    return util.WriteBytes(s.writer, bytes)
}

func (s *{{ $writerImpl }}) Close() {

}

var _ {{ $writerInt }} = &{{ $writerImpl }}{}

{{- $streamWriterImpl := printf "%s%sStreamWriter" $serviceImpl .Name }}
{{- $newStreamWriter := printf "new%s%sStreamWriter" $serviceInt .Name }}

func {{ $newStreamWriter }}(stream streams.WriteStream) {{ $writerInt }} {
    return &{{ $streamWriterImpl }}{
        stream: stream,
    }
}

type {{ $streamWriterImpl }} struct {
    stream streams.WriteStream
}

func (s *{{ $streamWriterImpl }}) Write(value *{{ $output }}) error {
    bytes, err := proto.Marshal(value)
    if err != nil {
        return err
    }
    s.stream.Value(bytes)
    return nil
}

func (s *{{ $streamWriterImpl }}) Close() {
    s.stream.Close()
}

var _ {{ $writerInt }} = &{{ $streamWriterImpl }}{}
{{- else }}
{{- $output := include "type" .Response.Output.Field.Type }}
{{- $streamInt := printf "%s%sStream" $serviceInt .Name }}
{{- $streamImpl := printf "%s%sStream" $serviceImpl .Name }}
{{- $newStream := printf "new%s%sStream" $serviceInt .Name }}
type {{ $streamInt }} interface {
	// Notify sends a value on the stream
	Notify(value *{{ $output }}) error

	// Close closes the stream
	Close()
}

func {{ $newStream }}(stream streams.WriteStream) {{ $streamInt }} {
    return &{{ $streamImpl }}{
        stream: stream,
    }
}

type {{ $streamImpl }} struct {
    stream streams.WriteStream
}

func (s *{{ $streamImpl }}) Notify(value *{{ $output }}) error {
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
{{- end }}
{{- else }}
{{- $informerInt := printf "%s%sInformer" $serviceInt .Name }}
{{- $informerImpl := printf "%s%sInformer" $serviceImpl .Name }}
{{- $newInformer := printf "new%s%sInformer" $serviceInt .Name }}
type {{ $informerInt }} interface {
	// Notify notifies the client
	Notify()

	// Close closes the stream
	Close()
}

func {{ $newInformer }}(stream streams.WriteStream) {{ $informerInt }} {
    return &{{ $informerImpl }}{
        stream: stream,
    }
}

type {{ $informerImpl }} struct {
    stream streams.WriteStream
}

func (s *{{ $informerImpl }}) Notify() error {
    s.stream.Result(null, null)
    return nil
}

func (s *{{ $informerImpl }}) Close() {
    s.stream.Close()
}

var _ {{ $informerInt }} = &{{ $informerImpl }}{}
{{- end }}
{{- else if .Type.IsAsync }}
{{- $futureImpl := printf "%sFuture" .Response.Output.Field.Type.Name }}
type {{ $futureImpl }} struct {
    stream streams.WriteStream
	{{- if .Response.Output }}
    output *{{ template "type" .Response.Output.Field.Type }}
    {{- else }}
    complete bool
    {{- end }}
    err error
}

func (f *{{ $futureImpl }}) setStream(stream streams.WriteStream) {
    {{- if .Response.Output }}
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
    {{- else }}
    if f.complete {
        stream.Close()
    } else if f.err != nil {
        stream.Error(f.err)
        stream.Close()
    } else {
        f.stream = stream
    }
    {{- end }}
}

{{ if .Response.Output }}
func (f *{{ $futureImpl }}) Complete(output *{{ template "type" .Response.Output.Field.Type }}) {
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
{{ else }}
func (f *{{ $futureImpl }}) Complete() {
    if f.stream != nil {
        f.stream.Close()
    } else {
        f.complete = true
    }
}
{{ end }}

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
    crdt.Service
    {{- range .Primitive.Methods }}
    {{- $streamInt := printf "%s%sStream" $serviceInt .Name }}
    {{- $informerInt := printf "%s%sInformer" $serviceInt .Name }}
    {{- $writerInt := printf "%s%sWriter" $serviceInt .Name }}
    {{- $comments := split .Comment "\n" }}
    {{- range $comment := $comments }}
    {{- if $comment }}
    // {{ $comment | trim }}
    {{- end }}
    {{- end }}
    {{- if .Response.IsDiscrete }}
    {{- if .Type.IsAsync }}
    {{- $futureImpl := printf "%sFuture" .Response.Output.Field.Type.Name }}
    {{- if .Request.Input }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Input.Field.Type }}) (*{{ $futureImpl }}, error)
    {{- else }}
    {{ .Name }}(context.Context) (*{{ $futureImpl }}, error)
    {{- end }}
    {{- else if and .Request.Input .Response.Output }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Input.Field.Type }}) (*{{ template "type" .Response.Output.Field.Type }}, error)
    {{- else if .Request.Input }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Input.Field.Type }}) error
    {{- else if .Response.Output }}
    {{ .Name }}(context.Context) (*{{ template "type" .Response.Output.Field.Type }}, error)
    {{- else }}
    {{ .Name }}(context.Context) error
    {{- end }}
    {{- else if .Response.IsStream }}
    {{- if .Type.IsSnapshot }}
    {{ .Name }}(context.Context, {{ $writerInt }}) error
    {{- else if and .Request.Input .Response.Output }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Input.Field.Type }}, {{ $streamInt }}) error
    {{- else if .Request.Input }}
    {{ .Name }}(context.Context, *{{ template "type" .Request.Input.Field.Type }}, {{ $informerInt }}) error
    {{- else if .Response.Output }}
    {{ .Name }}(context.Context, {{ $streamInt }}) error
    {{- else }}
    {{ .Name }}(context.Context, {{ $informerInt }}) error
    {{- end }}
    {{- end }}
    {{- end }}
}
