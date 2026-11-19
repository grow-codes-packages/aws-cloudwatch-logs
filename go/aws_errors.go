package cloudwatchlogs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// awsErrorCodes maps AWS error codes to canonical codes (API.md §1.1). AWS JSON
// services answer HTTP 400 for throttling and access denied, so the AWS code
// decides and the HTTP status is only the fallback.
var awsErrorCodes = map[string]ErrorCode{
	"ResourceNotFoundException":           ErrorCodeNotFound,
	"ResourceAlreadyExistsException":      ErrorCodeConflict,
	"OperationAbortedException":           ErrorCodeConflict,
	"DataAlreadyAcceptedException":        ErrorCodeConflict,
	"InvalidSequenceTokenException":       ErrorCodeConflict,
	"InvalidParameterException":           ErrorCodeValidation,
	"ValidationException":                 ErrorCodeValidation,
	"MalformedQueryException":             ErrorCodeValidation,
	"InvalidOperationException":           ErrorCodeValidation,
	"LimitExceededException":              ErrorCodeValidation,
	"AccessDeniedException":               ErrorCodePermissionDenied,
	"AccessDenied":                        ErrorCodePermissionDenied,
	"UnrecognizedClientException":         ErrorCodeAuthentication,
	"InvalidClientTokenId":                ErrorCodeAuthentication,
	"InvalidSignatureException":           ErrorCodeAuthentication,
	"SignatureDoesNotMatch":               ErrorCodeAuthentication,
	"IncompleteSignature":                 ErrorCodeAuthentication,
	"MissingAuthenticationToken":          ErrorCodeAuthentication,
	"MissingAuthenticationTokenException": ErrorCodeAuthentication,
	"ExpiredToken":                        ErrorCodeAuthentication,
	"ExpiredTokenException":               ErrorCodeAuthentication,
	"ThrottlingException":                 ErrorCodeRateLimited,
	"TooManyRequestsException":            ErrorCodeRateLimited,
	"ServiceUnavailableException":         ErrorCodeUnavailable,
	"InternalFailure":                     ErrorCodeUnavailable,
	"SessionStreamingException":           ErrorCodeUnavailable,
	"SessionTimeoutException":             ErrorCodeTimeout,
}

// retryableAWSCodes overrides the code's default retryability.
var retryableAWSCodes = map[string]bool{
	"OperationAbortedException": true,
	"SessionStreamingException": true,
	"SessionTimeoutException":   true,
}

// authMarkers identify an authentication failure in an HTTP 403 body without a
// parseable AWS error code (Moto answers IAM errors with XML on a JSON protocol).
var authMarkers = []string{"InvalidClientTokenId", "SignatureDoesNotMatch", "security token", "UnrecognizedClient"}

// mapError converts an AWS SDK error into an *Error (SPEC §2.1). op is the Grow
// operation name ("putLogEvents app/web"). Messages are redacted with the
// client's known secrets and never contain request bodies.
func (c *Client) mapError(op string, err error) error {
	if err == nil {
		return nil
	}
	var ge *Error
	if errors.As(err, &ge) {
		return ge
	}
	switch {
	case errors.Is(err, context.Canceled):
		return &Error{Code: ErrorCodeCancelled, Message: op + ": cancelled", Err: context.Canceled}
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Code: ErrorCodeTimeout, Message: op + ": deadline exceeded", Retryable: true, Err: context.DeadlineExceeded}
	}

	status, reqID := 0, ""
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		status, reqID = re.HTTPStatusCode(), re.ServiceRequestID()
	} else {
		var sre *smithyhttp.ResponseError
		if errors.As(err, &sre) {
			status = sre.HTTPStatusCode()
			if sre.Response != nil {
				reqID = requestID(sre.Response.Header)
			}
		}
	}
	retryAfter, _ := retryAfterOf(err, time.Now())

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" && apiErr.ErrorCode() != "UnknownError" {
		awsCode := apiErr.ErrorCode()
		if i := strings.LastIndex(awsCode, "#"); i >= 0 {
			awsCode = awsCode[i+1:]
		}
		code, known := awsErrorCodes[awsCode]
		if !known {
			switch {
			case status > 0:
				code = codeForStatus(status, "")
			case apiErr.ErrorFault() == smithy.FaultServer:
				code = ErrorCodeUnavailable
			default:
				code = ErrorCodeValidation
			}
		}
		e := c.newError(op, code, status, reqID, retryAfter, awsCode, c.redact(apiErr.ErrorMessage()))
		if r, ok := retryableAWSCodes[awsCode]; ok {
			e.Retryable = r
		}
		return e
	}

	var de *smithy.DeserializationError
	if errors.As(err, &de) && (status == 0 || status < 300) {
		return &Error{Code: ErrorCodeUnavailable, Message: op + ": invalid response from the service", HTTPStatus: status, RequestID: reqID,
			Retryable: false, Err: errors.New(truncate(c.redact(de.Error()), maxVendorText))}
	}
	if status > 0 {
		body := ""
		if de != nil {
			body = string(de.Snapshot)
		}
		if body == "" && apiErr != nil {
			body = apiErr.ErrorMessage()
		}
		code := codeForStatus(status, body)
		return c.newError(op, code, status, reqID, retryAfter, "", statusReason(status))
	}

	var se *v4.SigningError
	if errors.As(err, &se) || strings.Contains(err.Error(), "retrieve credentials") {
		return &Error{Code: ErrorCodeAuthentication, Message: op + ": no usable AWS credentials found", Err: errors.New(c.redact(err.Error()))}
	}
	var mre *aws.MissingRegionError
	if errors.As(err, &mre) {
		return ConfigError("region is not set (GROW_AWS_CLOUDWATCH_LOGS_REGION or AWS_REGION)")
	}
	var rte *awshttp.ResponseTimeoutError
	var nerr net.Error
	if errors.As(err, &rte) || (errors.As(err, &nerr) && nerr.Timeout()) {
		return &Error{Code: ErrorCodeTimeout, Message: op + ": request timeout exceeded", Retryable: true, Err: errors.New(c.redact(innerMessage(err)))}
	}
	var send *smithyhttp.RequestSendError
	if errors.As(err, &send) {
		msg := c.redact(innerMessage(send.Err))
		return &Error{Code: ErrorCodeUnavailable, Message: op + ": network error: " + msg, Retryable: true, Err: errors.New(msg)}
	}
	msg := c.redact(err.Error())
	return &Error{Code: ErrorCodeUnavailable, Message: op + ": " + truncate(msg, maxVendorText), Retryable: true, Err: errors.New(msg)}
}

// codeForStatus maps a status without a recognised AWS error code.
func codeForStatus(status int, body string) ErrorCode {
	switch status {
	case 403:
		for _, m := range authMarkers {
			if strings.Contains(body, m) {
				return ErrorCodeAuthentication
			}
		}
		return ErrorCodePermissionDenied
	case 0:
		return ErrorCodeUnavailable
	case 400:
		return ErrorCodeValidation
	}
	return CodeForStatus(status)
}

func (c *Client) newError(op string, code ErrorCode, status int, reqID string, retryAfter time.Duration, awsCode, msg string) *Error {
	reason := awsCode
	msg = strings.Join(strings.Fields(msg), " ")
	switch {
	case reason == "":
		reason = msg
	case msg != "" && msg != awsCode:
		reason += ": " + truncate(msg, maxVendorText)
	}
	text := op + ": " + reason
	if status > 0 {
		text += fmt.Sprintf(" (HTTP %d)", status)
	}
	e := &Error{Code: code, Message: text, HTTPStatus: status, RequestID: reqID, Retryable: code.Retryable(), RetryAfter: retryAfter}
	if awsCode != "" {
		e.Err = &smithy.GenericAPIError{Code: awsCode, Message: msg}
	}
	return e
}

// redact removes every known secret from s.
func (c *Client) redact(s string) string { return RedactText(s, c.secrets) }

// AWSErrorCode returns the AWS error code behind err ("ResourceNotFoundException"),
// or "" when err did not come from an AWS error response.
func AWSErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}
