package xwal

import "fmt"

// XWALErrorTypes is an enum for the different types of errors that can be returned by the xwal package.
type XWALErrorTypes uint8

const (

	// BadConfiguration is returned when the configuration passed to xWAL is invalid.
	XWALBadConfiguration = iota

	// DirectoryNotFound is returned when the directory specified in the configuration does not exist.
	XWALDirectoryNotFound

	// InvalidBufferSize is returned when the buffer size specified in the configuration is invalid.
	XWALInvalidBufferSize

	// InvalidFlushFrequency is returned when the flush frequency specified in the configuration is invalid.
	XWALInvalidFlushFrequency

	// InvalidSegmentSize is returned when the segment size specified in the configuration is invalid.
	XWALInvalidSegmentSize

	// InvalidFileSize is returned when the file size specified in the configuration is invalid.
	XWALInvalidFileSize
)

// XWALError is a custom error type that is returned by the xwal package.
type XWALError struct {
	// Type is the type of error that occurred.
	Type XWALErrorTypes
	// Message is a human-readable message that describes the error.
	Message string
	// Context is a map of key-value pairs that provide additional context for the error.
	Context map[string]string
}

// NewXWALError creates a new XWALError with the specified type, message, and context.
func NewXWALError(t XWALErrorTypes, m string, c map[string]string) *XWALError {
	return &XWALError{
		Type:    t,
		Message: m,
		Context: c,
	}
}

// Error returns a string representation of the error in respect to Golang Error interface.
func (e *XWALError) Error() string {
	return fmt.Sprintf("Type: %v, Message: %s, Context: %v", e.Type, e.Message, e.Context)
}
