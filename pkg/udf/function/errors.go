package function

import "fmt"

// ApplyUDFErr represents any UDF related error
type ApplyUDFErr struct {
	UserUDFErr bool
	Message    string
	InternalErr
}

// InternalErr represents errors internal to the platform
type InternalErr struct {
	Flag        bool
	MainCarDown bool
}

// IsUserUDFErr is true if the problem is due to the user code in the UDF.
func (e ApplyUDFErr) IsUserUDFErr() bool {
	return e.UserUDFErr
}

// IsInternalErr is true if this is a platform issue. This is a blocking error.
func (e ApplyUDFErr) IsInternalErr() bool {
	return e.InternalErr.Flag
}

func (e ApplyUDFErr) Error() string {
	return fmt.Sprint(e.Message)
}
