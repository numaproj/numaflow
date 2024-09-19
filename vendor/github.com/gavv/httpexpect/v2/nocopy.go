package httpexpect

// noCopy struct is a special type that is used to prevent a value from being copied.
// `go vet` gives a warning if it finds that a struct with a field of
// this type is being copied.
// To enable this behavior, this struct provides two methods `Lock` and `Unlock,
// that do not do anything.
// See more details here:
//   https://github.com/golang/go/issues/8005
//   https://stackoverflow.com/questions/52494458

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
