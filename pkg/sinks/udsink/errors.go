package udsink

import "fmt"

// ApplyUDSinkErr represents any UDSink related error
type ApplyUDSinkErr struct {
	UserUDSinkErr bool
	Message       string
	InternalErr
}

// InternalErr represents errors internal to the platform
type InternalErr struct {
	Flag        bool
	MainCarDown bool
}

// IsUserUDSinkErr is true if the problem is due to the user code in the UDSink.
func (e ApplyUDSinkErr) IsUserUDSinkErr() bool {
	return e.UserUDSinkErr
}

// IsInternalErr is true if this is a platform issue. This is a blocking error.
func (e ApplyUDSinkErr) IsInternalErr() bool {
	return e.InternalErr.Flag
}

func (e ApplyUDSinkErr) Error() string {
	return fmt.Sprint(e.Message)
}
