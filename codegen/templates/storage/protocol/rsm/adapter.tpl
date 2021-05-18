// Code generated by atomix-go-framework. DO NOT EDIT.
package {{ .Package.Name }}

{{ $serviceInt := printf "%sService" .Generator.Prefix }}
{{- $serviceImpl := printf "%sServiceAdaptor" .Generator.Prefix }}

{{ define "type" }}
{{- if .Package.Import -}}
{{- printf "%s.%s" .Package.Alias .Name -}}
{{- else -}}
{{- .Name -}}
{{- end -}}
{{- end -}}

import (
    "io"
	"github.com/atomix/atomix-go-framework/pkg/atomix/storage/protocol/rsm"
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
	"github.com/atomix/atomix-go-framework/pkg/atomix/logging"
	"github.com/golang/protobuf/proto"
	{{- $package := .Package }}
	{{- range .Imports }}
	{{ .Alias }} {{ .Path | quote }}
	{{- end }}
)

var log = logging.GetLogger("atomix", {{ .Primitive.Name | lower | quote }}, "service")

const {{ printf "%sType" .Generator.Prefix }} = {{ .Primitive.Name | quote }}
{{ $root := . }}
const (
    {{- range .Primitive.Methods }}
    {{ (printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel }} = {{ .Name | quote }}
    {{- end }}
)

{{ $serviceContextInt := printf "%sServiceContext" .Generator.Prefix }}
var new{{ $serviceInt }}Func rsm.NewServiceFunc

func register{{ $serviceInt }}Func(rsmf New{{ $serviceInt }}Func) {
	new{{ $serviceInt }}Func = func(scheduler rsm.Scheduler, context rsm.ServiceContext) rsm.Service {
		service := &{{ $serviceImpl }}{
			Service: rsm.NewService(scheduler, context),
			rsm:     rsmf(new{{ $serviceContextInt }}(scheduler)),
		}
		service.init()
		return service
	}
}

type New{{ $serviceInt }}Func func({{ $serviceContextInt }}) {{ $serviceInt }}

// Register{{ $serviceInt }} registers the election primitive service on the given node
func Register{{ $serviceInt }}(node *rsm.Node) {
	node.RegisterService({{ printf "%sType" .Generator.Prefix }}, new{{ $serviceInt }}Func)
}

type {{ $serviceImpl }} struct {
	rsm.Service
	rsm {{ $serviceInt }}
}

func (s *{{ $serviceImpl }}) init() {
    {{- range .Primitive.Methods }}
    {{- $name := ((printf "%s%sOp" $root.Generator.Prefix .Name) | toLowerCamel) }}
    {{- $op := ( .Name | toLowerCamel ) }}
    {{- if ( and .Response.IsUnary .Type.IsSync ) }}
	s.RegisterUnaryOperation({{ $name }}, s.{{ $op }})
	{{- else }}
	s.RegisterStreamOperation({{ $name }}, s.{{ $op }})
    {{- end }}
    {{- end }}
}

{{- $serviceProposalID := printf "%sProposalID" .Generator.Prefix }}
{{- $newServiceSession := printf "new%sSession" .Generator.Prefix }}
{{- $serviceSessionID := printf "%sSessionID" .Generator.Prefix }}
{{- $newServiceSnapshotWriter := printf "new%sSnapshotWriter" .Generator.Prefix }}
{{- $newServiceSnapshotReader := printf "new%sSnapshotReader" .Generator.Prefix }}
func (s *{{ $serviceImpl }}) SessionOpen(rsmSession rsm.Session) {
    s.rsm.Sessions().open({{ $newServiceSession }}(rsmSession))
}

func (s *{{ $serviceImpl }}) SessionExpired(session rsm.Session) {
    s.rsm.Sessions().expire({{ $serviceSessionID }}(session.ID()))
}

func (s *{{ $serviceImpl }}) SessionClosed(session rsm.Session) {
    s.rsm.Sessions().close({{ $serviceSessionID }}(session.ID()))
}

{{- if .Primitive.State }}
func (s *{{ $serviceImpl }}) Backup(writer io.Writer) error {
    err := s.rsm.Backup({{ $newServiceSnapshotWriter }}(writer))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *{{ $serviceImpl }}) Restore(reader io.Reader) error {
    err := s.rsm.Restore({{ $newServiceSnapshotReader }}(reader))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}
{{- end }}

{{- range .Primitive.Methods }}
{{- $proposalInt := printf "%sProposal" .Name }}
{{- $newProposal := printf "new%sProposal" .Name }}
{{- if ( and .Response.IsUnary .Type.IsSync ) }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(input []byte, rsmSession rsm.Session) ([]byte, error) {
    request := &{{ template "type" .Request.Type }}{}
	err := proto.Unmarshal(input, request)
	if err != nil {
	    log.Error(err)
		return nil, err
	}

    session, ok := s.rsm.Sessions().Get({{ $serviceSessionID }}(rsmSession.ID()))
    if !ok {
        err := errors.NewConflict("session %d not found", rsmSession.ID())
        log.Error(err.Error())
        return nil, err
    }

    proposal := {{ $newProposal }}({{ $serviceProposalID }}(s.Index()), session, request)

    s.rsm.Proposals().{{ .Name }}().register(proposal)
    session.Proposals().{{ .Name }}().register(proposal)

    defer func() {
        session.Proposals().{{ .Name }}().unregister(proposal.ID())
        s.rsm.Proposals().{{ .Name }}().unregister(proposal.ID())
    }()

    log.Debugf("Proposing {{ $proposalInt }} %s", proposal)
	err = s.rsm.{{ .Name }}(proposal)
	if err !=  nil {
	    log.Error(err.Error())
    	return nil, err
	}

	output, err := proto.Marshal(proposal.response())
	if err != nil {
	    log.Error(err)
		return nil, err
	}
	return output, nil
}
{{- else }}
func (s *{{ $serviceImpl }}) {{ .Name | toLowerCamel }}(input []byte, rsmSession rsm.Session, stream rsm.Stream) (rsm.StreamCloser, error) {
    request := &{{ template "type" .Request.Type }}{}
    err := proto.Unmarshal(input, request)
    if err != nil {
        log.Error(err)
        return nil, err
    }

    session, ok := s.rsm.Sessions().Get({{ $serviceSessionID }}(rsmSession.ID()))
    if !ok {
        err := errors.NewConflict("session %d not found", rsmSession.ID())
        log.Error(err.Error())
        return nil, err
    }

    proposal := {{ $newProposal }}({{ $serviceProposalID }}(stream.ID()), session, request, stream)

    s.rsm.Proposals().{{ .Name }}().register(proposal)
    session.Proposals().{{ .Name }}().register(proposal)

    log.Debugf("Proposing {{ $proposalInt }} %s", proposal)
    err = s.rsm.{{ .Name }}(proposal)
    if err != nil {
        log.Error(err.Error())
        return nil, err
    }
    return func() {
        session.Proposals().{{ .Name }}().unregister(proposal.ID())
        s.rsm.Proposals().{{ .Name }}().unregister(proposal.ID())
    }, nil
}
{{ end }}
{{- end }}

var _ rsm.Service = &{{ $serviceImpl }}{}
