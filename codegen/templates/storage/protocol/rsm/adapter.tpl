// Code generated by atomix-go-framework. DO NOT EDIT.

// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/gogo/protobuf/proto"
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
	new{{ $serviceInt }}Func = func(context rsm.ServiceContext) rsm.Service {
		return &{{ $serviceImpl }}{
			ServiceContext: context,
			rsm:            rsmf(new{{ $serviceContextInt }}(context)),
		}
	}
}

type New{{ $serviceInt }}Func func({{ $serviceContextInt }}) {{ $serviceInt }}

// Register{{ $serviceInt }} registers the election primitive service on the given node
func Register{{ $serviceInt }}(node *rsm.Node) {
	node.RegisterService({{ printf "%sType" .Generator.Prefix }}, new{{ $serviceInt }}Func)
}

type {{ $serviceImpl }} struct {
	rsm.ServiceContext
	rsm {{ $serviceInt }}
}

{{- $serviceProposalID := printf "%sProposalID" .Generator.Prefix }}
{{- $newServiceSession := printf "new%sSession" .Generator.Prefix }}
{{- $serviceSessionID := printf "%sSessionID" .Generator.Prefix }}
{{- $newServiceSnapshotWriter := printf "new%sSnapshotWriter" .Generator.Prefix }}
{{- $newServiceSnapshotReader := printf "new%sSnapshotReader" .Generator.Prefix }}

func (s *{{ $serviceImpl }}) ExecuteCommand(command rsm.Command) {
    switch command.OperationID() {
    {{- range .Primitive.Methods }}
    {{- if .Type.IsCommand }}
    {{- $proposalInt := printf "%sProposal" .Name }}
    {{- $newProposal := printf "new%sProposal" .Name }}
    case {{ .ID }}:
        p, err := {{ $newProposal }}(command)
        if err != nil {
            err = errors.NewInternal(err.Error())
            log.Error(err)
            command.Output(nil, err)
            return
        }

        log.Debugf("Proposal {{ $proposalInt }} %.250s", p)
        {{- if (and .Response.IsUnary .Type.IsSync ) }}
        response, err := s.rsm.{{ .Name }}(p)
        if err != nil {
            log.Debugf("Proposal {{ $proposalInt }} %.250s failed: %v", p, err)
            command.Output(nil, err)
        } else {
            output, err := proto.Marshal(response)
            if err != nil {
                err = errors.NewInternal(err.Error())
                log.Errorf("Proposal {{ $proposalInt }} %.250s failed: %v", p, err)
                command.Output(nil, err)
            } else {
                log.Debugf("Proposal {{ $proposalInt }} %.250s complete: %.250s", p, response)
                command.Output(output, nil)
            }
        }
        command.Close()
        {{- else }}
        s.rsm.{{ .Name }}(p)
        {{- end }}
    {{- end }}
    {{- end }}
    default:
        err := errors.NewNotSupported("unknown operation %d", command.OperationID())
        log.Debug(err)
        command.Output(nil, err)
    }
}

func (s *{{ $serviceImpl }}) ExecuteQuery(query rsm.Query) {
    switch query.OperationID() {
    {{- range .Primitive.Methods }}
    {{- if .Type.IsQuery }}
    {{- $queryInt := printf "%sQuery" .Name }}
    {{- $newQuery := printf "new%sQuery" .Name }}
    case {{ .ID }}:
        q, err := {{ $newQuery }}(query)
        if err != nil {
            err = errors.NewInternal(err.Error())
            log.Error(err)
            query.Output(nil, err)
            return
        }

        log.Debugf("Querying {{ $queryInt }} %.250s", q)
        {{- if .Response.IsUnary }}
        response, err := s.rsm.{{ .Name }}(q)
        if err != nil {
            log.Debugf("Querying {{ $queryInt }} %.250s failed: %v", q, err)
            query.Output(nil, err)
        } else {
            output, err := proto.Marshal(response)
            if err != nil {
                err = errors.NewInternal(err.Error())
                log.Errorf("Querying {{ $queryInt }} %.250s failed: %v", q, err)
                query.Output(nil, err)
            } else {
                log.Debugf("Querying {{ $queryInt }} %.250s complete: %+v", q, response)
                query.Output(output, nil)
            }
        }
        query.Close()
        {{- else if .Response.IsStream }}
        s.rsm.{{ .Name }}(q)
        {{- end }}
    {{- end }}
    {{- end }}
    default:
        err := errors.NewNotSupported("unknown operation %d", query.OperationID())
        log.Debug(err)
        query.Output(nil, err)
    }
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

var _ rsm.Service = &{{ $serviceImpl }}{}
