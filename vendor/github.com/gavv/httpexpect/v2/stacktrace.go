package httpexpect

import (
	"regexp"
	"runtime"
)

// Stacktrace entry.
type StacktraceEntry struct {
	Pc uintptr // Program counter

	File string // File path
	Line int    // Line number

	Func *runtime.Func // Function information

	FuncName    string  // Function name (without package and parenthesis)
	FuncPackage string  // Function package
	FuncOffset  uintptr // Program counter offset relative to function start

	// True if this is program entry point
	// (like main.main or testing.tRunner)
	IsEntrypoint bool
}

var stacktraceFuncRe = regexp.MustCompile(`^(.+/[^.]+)\.(.+)$`)

func stacktrace() []StacktraceEntry {
	callers := []StacktraceEntry{}
	for i := 1; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		f := runtime.FuncForPC(pc)
		if f == nil {
			break
		}

		entry := StacktraceEntry{
			Pc:   pc,
			File: file,
			Line: line,
			Func: f,
		}

		if m := stacktraceFuncRe.FindStringSubmatch(f.Name()); m != nil {
			entry.FuncName = m[2]
			entry.FuncPackage = m[1]
		} else {
			entry.FuncName = f.Name()
		}

		entry.FuncOffset = pc - f.Entry()

		switch f.Name() {
		case "main.main", "testing.tRunner":
			entry.IsEntrypoint = true
		}

		callers = append(callers, entry)
	}

	return callers
}
