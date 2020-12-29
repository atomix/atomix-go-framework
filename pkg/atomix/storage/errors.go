// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"github.com/atomix/go-framework/pkg/atomix/errors"
)

// GetErrorFromStatus creates a typed error from a response status
func GetErrorFromStatus(status SessionResponseStatus) error {
	switch status.Code {
	case SessionResponseCode_OK:
		return nil
	case SessionResponseCode_ERROR:
		return errors.NewUnknown(status.Message)
	case SessionResponseCode_UNKNOWN:
		return errors.NewUnknown(status.Message)
	case SessionResponseCode_CANCELED:
		return errors.NewCanceled(status.Message)
	case SessionResponseCode_NOT_FOUND:
		return errors.NewNotFound(status.Message)
	case SessionResponseCode_ALREADY_EXISTS:
		return errors.NewAlreadyExists(status.Message)
	case SessionResponseCode_UNAUTHORIZED:
		return errors.NewUnauthorized(status.Message)
	case SessionResponseCode_FORBIDDEN:
		return errors.NewForbidden(status.Message)
	case SessionResponseCode_CONFLICT:
		return errors.NewConflict(status.Message)
	case SessionResponseCode_INVALID:
		return errors.NewInvalid(status.Message)
	case SessionResponseCode_UNAVAILABLE:
		return errors.NewUnavailable(status.Message)
	case SessionResponseCode_NOT_SUPPORTED:
		return errors.NewNotSupported(status.Message)
	case SessionResponseCode_TIMEOUT:
		return errors.NewTimeout(status.Message)
	case SessionResponseCode_INTERNAL:
		return errors.NewInternal(status.Message)
	default:
		return errors.NewUnknown(status.Message)
	}
}

// getStatus gets the proto status for the given error
func getCode(err error) SessionResponseCode {
	if err == nil {
		return SessionResponseCode_OK
	}

	typed, ok := err.(*errors.TypedError)
	if !ok {
		return SessionResponseCode_ERROR
	}

	switch typed.Type {
	case errors.Unknown:
		return SessionResponseCode_UNKNOWN
	case errors.Canceled:
		return SessionResponseCode_CANCELED
	case errors.NotFound:
		return SessionResponseCode_NOT_FOUND
	case errors.AlreadyExists:
		return SessionResponseCode_ALREADY_EXISTS
	case errors.Unauthorized:
		return SessionResponseCode_UNAUTHORIZED
	case errors.Forbidden:
		return SessionResponseCode_FORBIDDEN
	case errors.Conflict:
		return SessionResponseCode_CONFLICT
	case errors.Invalid:
		return SessionResponseCode_INVALID
	case errors.Unavailable:
		return SessionResponseCode_UNAVAILABLE
	case errors.NotSupported:
		return SessionResponseCode_NOT_SUPPORTED
	case errors.Timeout:
		return SessionResponseCode_TIMEOUT
	case errors.Internal:
		return SessionResponseCode_INTERNAL
	default:
		return SessionResponseCode_ERROR
	}
}

// getMessage gets the message for the given error
func getMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
