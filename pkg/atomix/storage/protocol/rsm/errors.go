// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package rsm

import (
	"github.com/atomix/atomix-go-framework/pkg/atomix/errors"
)

// GetErrorFromStatus creates a typed error from a response status
func GetErrorFromStatus(status ResponseStatus) error {
	switch status.Code {
	case ResponseCode_OK:
		return nil
	case ResponseCode_ERROR:
		return errors.NewUnknown(status.Message)
	case ResponseCode_UNKNOWN:
		return errors.NewUnknown(status.Message)
	case ResponseCode_CANCELED:
		return errors.NewCanceled(status.Message)
	case ResponseCode_NOT_FOUND:
		return errors.NewNotFound(status.Message)
	case ResponseCode_ALREADY_EXISTS:
		return errors.NewAlreadyExists(status.Message)
	case ResponseCode_UNAUTHORIZED:
		return errors.NewUnauthorized(status.Message)
	case ResponseCode_FORBIDDEN:
		return errors.NewForbidden(status.Message)
	case ResponseCode_CONFLICT:
		return errors.NewConflict(status.Message)
	case ResponseCode_INVALID:
		return errors.NewInvalid(status.Message)
	case ResponseCode_UNAVAILABLE:
		return errors.NewUnavailable(status.Message)
	case ResponseCode_NOT_SUPPORTED:
		return errors.NewNotSupported(status.Message)
	case ResponseCode_TIMEOUT:
		return errors.NewTimeout(status.Message)
	case ResponseCode_INTERNAL:
		return errors.NewInternal(status.Message)
	default:
		return errors.NewUnknown(status.Message)
	}
}

// getStatus gets the proto status for the given error
func getCode(err error) ResponseCode {
	if err == nil {
		return ResponseCode_OK
	}

	typed, ok := err.(*errors.TypedError)
	if !ok {
		return ResponseCode_ERROR
	}

	switch typed.Type {
	case errors.Unknown:
		return ResponseCode_UNKNOWN
	case errors.Canceled:
		return ResponseCode_CANCELED
	case errors.NotFound:
		return ResponseCode_NOT_FOUND
	case errors.AlreadyExists:
		return ResponseCode_ALREADY_EXISTS
	case errors.Unauthorized:
		return ResponseCode_UNAUTHORIZED
	case errors.Forbidden:
		return ResponseCode_FORBIDDEN
	case errors.Conflict:
		return ResponseCode_CONFLICT
	case errors.Invalid:
		return ResponseCode_INVALID
	case errors.Unavailable:
		return ResponseCode_UNAVAILABLE
	case errors.NotSupported:
		return ResponseCode_NOT_SUPPORTED
	case errors.Timeout:
		return ResponseCode_TIMEOUT
	case errors.Internal:
		return ResponseCode_INTERNAL
	default:
		return ResponseCode_ERROR
	}
}

// getMessage gets the message for the given error
func getMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
