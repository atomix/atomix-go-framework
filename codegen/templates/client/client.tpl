{{- $clientInt := (printf "%sClient" .Generator.Prefix) }}
{{- $clientImpl := ((printf "%s%sClient" .Generator.Prefix .Primitive.Name) | toLowerCamel) }}

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

{{- define "deref" -}}
{{- if not .Field.Type.IsPointer }}*{{ end }}
{{- end }}

{{- define "val" -}}
{{- if .Field.Type.IsPointer }}*{{ end }}
{{- end }}

package {{ .Package.Name }}

import (
    "context"
	"github.com/atomix/go-framework/pkg/atomix/logging"
	"github.com/atomix/go-framework/pkg/atomix/client"
	"github.com/atomix/go-framework/pkg/atomix/errors"
	primitiveapi "github.com/atomix/api/go/atomix/primitive"
	"google.golang.org/grpc"
	{{- $added := false }}
	{{- range .Primitive.Methods }}
	{{- if and (not $added) .Response.IsStream }}
	"io"
	{{- $added = true }}
	{{- end }}
	{{- end }}
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

const {{ printf "%sPrimitiveType" .Generator.Prefix }} client.PrimitiveType = {{ .Primitive.Name | quote }}

func New{{ $clientInt }}(id client.ID, name string, conn *grpc.ClientConn) {{ $clientInt }} {
    return &{{ $clientImpl }}{
        PrimitiveClient: client.NewPrimitiveClient(id, {{ printf "%sPrimitiveType" .Generator.Prefix }}, name, conn),
        client: {{ .Primitive.Type.Package.Alias }}.New{{ .Primitive.Type.Name }}Client(conn),
        log: logging.GetLogger("atomix", "client", {{ .Primitive.Name | lower | quote }}),
    }
}

type {{ $clientInt }} interface {
    client.PrimitiveClient
    {{- range .Primitive.Methods }}
    {{- $comments := split .Comment "\n" }}
    {{- range $comment := $comments }}
    {{- if $comment }}
    // {{ $comment | trim }}
    {{- end }}
    {{- end }}
    {{- if and .Request.IsDiscrete .Response.IsDiscrete }}
    {{- if and .Request.Input .Response.Output }}
    {{ .Name }}(context.Context, *{{ include "type" .Request.Input.Field.Type }}) (*{{ include "type" .Response.Output.Field.Type }}, error)
    {{- else if .Request.Input }}
    {{ .Name }}(context.Context, *{{ include "type" .Request.Input.Field.Type }}) error
    {{- else if .Response.Output }}
    {{ .Name }}(context.Context) (*{{ include "type" .Response.Output.Field.Type }}, error)
    {{- else }}
    {{ .Name }}(context.Context) error
    {{- end }}
    {{- else if .Response.IsStream }}
    {{- if and .Request.Input .Response.Output }}
    {{ .Name }}(context.Context, *{{ include "type" .Request.Input.Field.Type }}, chan<- {{ template "type" .Response.Output.Field.Type }}) error
    {{- else if .Request.Input }}
    {{ .Name }}(context.Context, *{{ include "type" .Request.Input.Field.Type }}, chan<- struct{}) error
    {{- else if .Response.Output }}
    {{ .Name }}(context.Context, chan<- {{ template "type" .Response.Output.Field.Type }}) error
    {{- else }}
    {{ .Name }}(context.Context, chan<- struct{}) error
    {{- end }}
    {{- end }}
    {{- end }}
}

type {{ $clientImpl }} struct {
    client.PrimitiveClient
    client {{ .Primitive.Type.Package.Alias }}.{{ .Primitive.Type.Name }}Client
	log logging.Logger
}

func (c *{{ $clientImpl }}) getRequestHeader() primitiveapi.RequestHeader {
	return primitiveapi.RequestHeader{
		PrimitiveID: primitiveapi.PrimitiveId{
            Type: string(c.Type()),
		    Name: c.Name(),
		},
	}
}

{{- range .Primitive.Methods }}
{{ if and .Request.IsDiscrete .Response.IsDiscrete }}
{{- if and .Request.Input .Response.Output }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context, input *{{ include "type" .Request.Input.Field.Type }}) (*{{ include "type" .Response.Output.Field.Type }}, error) {
	request := &{{ include "type" .Request.Type }}{
		Header: c.getRequestHeader(),
	}
	request{{ template "field" .Request.Input }} = {{ template "deref" .Request.Input }}input
	response, err := c.client.{{ .Name }}(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return {{ template "ref" .Response.Output }}response{{ template "field" .Response.Output }}, nil
}
{{- else if .Request.Input }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context, input *{{ include "type" .Request.Input.Field.Type }}) error {
	request := &{{ include "type" .Request.Type }}{
		Header: c.getRequestHeader(),
	}
	request{{ template "field" .Request.Input }} = {{ template "deref" .Request.Input }}input
	_, err := c.client.{{ .Name }}(ctx, request)
	if err != nil {
	    return errors.From(err)
	}
	return nil
}
{{- else if .Response.Output }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context) (*{{ include "type" .Response.Output.Field.Type }}, error) {
	request := &{{ include "type" .Request.Type }}{
		Header: c.getRequestHeader(),
	}
	response, err := c.client.{{ .Name }}(ctx, request)
	if err != nil {
		return nil, errors.From(err)
	}
	return {{ template "ref" .Response.Output }}response{{ template "field" .Response.Output }}, nil
}
{{- else }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context) error {
	request := &{{ include "type" .Request.Type }}{
		Header: c.getRequestHeader(),
	}
	_, err := c.client.{{ .Name }}(ctx, request)
	if err != nil {
	    return errors.From(err)
	}
	return nil
}
{{- end }}
{{ else if .Response.IsStream }}
{{- if and .Request.Input .Response.Output }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context, input *{{ include "type" .Request.Input.Field.Type }}, ch chan<- {{ template "type" .Response.Output.Field.Type }}) error {
    request := &{{ include "type" .Request.Type }}{
        Header: c.getRequestHeader(),
    }
    request{{ template "field" .Request.Input }} = {{ template "deref" .Request.Input }}input

	stream, err := c.client.{{ .Name }}(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	handshakeCh := make(chan struct{})
	go func() {
		defer close(ch)
		response, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			c.log.Error(err)
		} else {
			switch response.Header.ResponseType {
			case primitiveapi.ResponseType_RESPONSE:
                ch <- {{ template "val" .Response.Output }}response{{ template "field" .Response.Output }}
			case primitiveapi.ResponseType_RESPONSE_STREAM:
				close(handshakeCh)
			}
		}
	}()

	select {
	case <-handshakeCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
{{- else if .Request.Input }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context, input *{{ include "type" .Request.Input.Field.Type }}, ch chan<- struct{}) error {
    request := &{{ include "type" .Request.Type }}{
        Header: c.getRequestHeader(),
    }
    request{{ template "field" .Request.Input }} = {{ template "deref" .Request.Input }}input

    stream, err := c.client.{{ .Name }}(ctx, request)
    if err != nil {
        return errors.From(err)
    }

    handshakeCh := make(chan struct{})
    go func() {
        defer close(ch)
        response, err := stream.Recv()
        if err == io.EOF {
            return
        }
        if err != nil {
            c.log.Error(err)
        } else {
            switch response.Header.ResponseType {
            case primitiveapi.ResponseType_RESPONSE:
                ch <- struct{}{}
            case primitiveapi.ResponseType_RESPONSE_STREAM:
                close(handshakeCh)
            }
        }
    }()

    select {
    case <-handshakeCh:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
{{- else if .Response.Output }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context, ch chan<- {{ template "type" .Response.Output.Field.Type }}) error {
    request := &{{ include "type" .Request.Type }}{
        Header: c.getRequestHeader(),
    }

	stream, err := c.client.{{ .Name }}(ctx, request)
	if err != nil {
		return errors.From(err)
	}

	handshakeCh := make(chan struct{})
	go func() {
		defer close(ch)
		response, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			c.log.Error(err)
		} else {
			switch response.Header.ResponseType {
			case primitiveapi.ResponseType_RESPONSE:
                ch <- {{ template "val" .Response.Output }}response{{ template "field" .Response.Output }}
			case primitiveapi.ResponseType_RESPONSE_STREAM:
				close(handshakeCh)
			}
		}
	}()

	select {
	case <-handshakeCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
{{- else }}
func (c *{{ $clientImpl }}) {{ .Name }}(ctx context.Context, ch chan<- struct{}) error {
    request := &{{ include "type" .Request.Type }}{
        Header: c.getRequestHeader(),
    }

    stream, err := c.client.{{ .Name }}(ctx, request)
    if err != nil {
        return errors.From(err)
    }

    handshakeCh := make(chan struct{})
    go func() {
        defer close(ch)
        response, err := stream.Recv()
        if err == io.EOF {
            return
        }
        if err != nil {
            c.log.Error(err)
        } else {
            switch response.Header.ResponseType {
            case primitiveapi.ResponseType_RESPONSE:
                ch <- struct{}{}
            case primitiveapi.ResponseType_RESPONSE_STREAM:
                close(handshakeCh)
            }
        }
    }()

    select {
    case <-handshakeCh:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
{{- end }}
{{ end }}
{{- end }}

var _ {{ $clientInt }} = &{{ $clientImpl }}{}
