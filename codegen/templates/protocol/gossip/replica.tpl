{{- $serviceType := printf "%sServiceType" .Generator.Prefix }}
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

package {{ .Package.Name }}

import (
	"context"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
	{{- range .Primitive.Methods }}
	{{- if or .Type.IsAsync .Response.IsStream }}
	{{ import "github.com/golang/protobuf/proto" }}
	{{- end }}
	{{- end }}
	"github.com/atomix/go-framework/pkg/atomix/protocol/gossip"
)

var newReplica func(protocol Protocol) Replica

func registerReplica(f func(protocol Protocol) Replica) {
	newReplica = f
}

type Replica interface {
	Service() {{ $serviceInt }}
    {{- if .Primitive.State.Value }}
	Read(ctx context.Context) (*{{ template "type" .Primitive.State.Value.Type }}, error)
	Update(ctx context.Context, value *{{ template "type" .Primitive.State.Value.Type }}) error
    {{- else if .Primitive.State.Entry }}
    {{- if .Primitive.State.Entry.Key }}
    {{- if .Primitive.State.Entry.Key.Field.Type.IsScalar }}
	Read(ctx context.Context, key {{ .Primitive.State.Entry.Key.Field.Type.Name }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
    {{- else }}
	Read(ctx context.Context, key {{ template "type" .Primitive.State.Entry.Key.Field.Type }}) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
	{{- end }}
    {{- else }}
	Read(ctx context.Context) (*{{ template "type" .Primitive.State.Entry.Type }}, error)
    {{- end }}
    List(ctx context.Context, ch chan<- {{ template "type" .Primitive.State.Entry.Type }}) error
	Update(ctx context.Context, entry *{{ template "type" .Primitive.State.Entry.Type }}) error
    {{- end }}
}

func newReplicaAdaptor(replica Replica) gossip.Replica {
    return &serviceReplicaAdaptor{
        replica: replica,
    }
}

{{ if .Primitive.State.Value }}
type serviceReplicaAdaptor struct {
	replica Replica
}

func (s *serviceReplicaAdaptor) Service() gossip.Service {
    return s.replica
}

func (s *serviceReplicaAdaptor) Read(ctx context.Context, _ string) (*gossip.Object, error) {
	value, err := s.replica.Read(ctx)
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
        {{- if .Primitive.State.Value.Digest }}
        ObjectMeta: value{{ template "field" .Primitive.State.Value.Digest }},
        {{- end }}
		Value:      bytes,
	}, nil
}

func (s *serviceReplicaAdaptor) Update(ctx context.Context, object *gossip.Object) error {
	value := &{{ template "type" .Primitive.State.Value.Type }}{}
	err := proto.Unmarshal(object.Value, value)
	if err != nil {
		return err
	}
	return s.replica.Update(ctx, value)
}

func (s *serviceReplicaAdaptor) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
    errCh := make(chan error)
    go func() {
        defer close(errCh)
        value, err := s.replica.Read(ctx)
        if err != nil {
            errCh <- err
            return
        }
        bytes, err := proto.Marshal(value)
        if err != nil {
            errCh <- err
            return
        }
        object := gossip.Object{
            {{- if .Primitive.State.Value.Digest }}
            ObjectMeta: value{{ template "field" .Primitive.State.Value.Digest }},
            {{- end }}
            Value:      bytes,
        }
        ch <- object
    }()
	return <-errCh
}
{{ else if .Primitive.State.Entry }}
type serviceReplicaAdaptor struct {
	replica Replica
}

func (s *serviceReplicaAdaptor) Service() gossip.Service {
    return s.replica
}

func (s *serviceReplicaAdaptor) Read(ctx context.Context, key string) (*gossip.Object, error) {
	entry, err := s.replica.Read(ctx, key)
	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	bytes, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}
	return &gossip.Object{
        {{- if .Primitive.State.Entry.Digest }}
        ObjectMeta: entry{{ template "field" .Primitive.State.Entry.Digest }},
        {{- end }}
        {{- if .Primitive.State.Entry.Key }}
        Key:        entry{{ template "field" .Primitive.State.Entry.Key }},
        {{- end }}
		Value:      bytes,
	}, nil
}

func (s *serviceReplicaAdaptor) Update(ctx context.Context, object *gossip.Object) error {
	entry := &{{ template "type" .Primitive.State.Entry.Type }}{}
	err := proto.Unmarshal(object.Value, entry)
	if err != nil {
		return err
	}
	return s.replica.Update(ctx, entry)
}

func (s *serviceReplicaAdaptor) ReadAll(ctx context.Context, ch chan<- gossip.Object) error {
	entriesCh := make(chan {{ template "type" .Primitive.State.Entry.Type }})
	errCh := make(chan error)
	go func() {
		err := s.replica.List(ctx, entriesCh)
		if err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer close(errCh)
		for entry := range entriesCh {
			bytes, err := proto.Marshal(&entry)
			if err != nil {
				errCh <- err
				return
			}
			object := gossip.Object{
			    {{- if .Primitive.State.Entry.Digest }}
				ObjectMeta: entry{{ template "field" .Primitive.State.Entry.Digest }},
				{{- end }}
				{{- if .Primitive.State.Entry.Key }}
				Key: entry{{ template "field" .Primitive.State.Entry.Key }},
				{{- end }}
				Value:      bytes,
			}
			ch <- object
		}
	}()
	return <-errCh
}
{{ end }}

var _ gossip.Replica = &serviceReplicaAdaptor{}
