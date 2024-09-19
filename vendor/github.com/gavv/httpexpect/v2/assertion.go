package httpexpect

// AssertionType defines type of performed assertion.
type AssertionType uint

//go:generate stringer -type=AssertionType
const (
	// Check if the invocation is correct
	AssertUsage AssertionType = iota

	// Check if the operation succeeded
	AssertOperation

	// Check expression: [Actual] has appropriate type
	AssertType
	AssertNotType

	// Check expression: [Actual] has valid value
	AssertValid
	AssertNotValid

	// Check expression: [Actual] is nil
	AssertNil
	AssertNotNil

	// Check expression: [Actual] is empty
	AssertEmpty
	AssertNotEmpty

	// Check expression: [Actual] is equal to [Expected]
	// If [Delta] is set, it specifies allowed difference between values
	AssertEqual
	AssertNotEqual

	// Check expression: [Actual] < [Expected]
	AssertLt
	// Check expression: [Actual] <= [Expected]
	AssertLe
	// Check expression: [Actual] > [Expected]
	AssertGt
	// Check expression: [Actual] >= [Expected]
	AssertGe

	// Check expression: [Actual] belongs to inclusive range [Expected]
	// [Expected] stores AssertionRange with Min and Max values
	AssertInRange
	AssertNotInRange

	// Check expression: [Actual] matches json schema [Expected]
	// [Expected] stores map with parsed schema or string with schema uri
	AssertMatchSchema
	AssertNotMatchSchema

	// Check expression: [Actual] matches json path [Expected]
	// [Expected] stores a string with json path
	AssertMatchPath
	AssertNotMatchPath

	// Check expression: [Actual] matches regex [Expected]
	// [Expected] stores a string with regular expression
	AssertMatchRegexp
	AssertNotMatchRegexp

	// Check expression: [Actual] matches format [Expected]
	// [Expected] stores expected format or format list (AssertionList)
	AssertMatchFormat
	AssertNotMatchFormat

	// Check expression: [Actual] contains key [Expected]
	AssertContainsKey
	AssertNotContainsKey

	// Check expression: [Actual] contains element [Expected]
	AssertContainsElement
	AssertNotContainsElement

	// Check expression: [Actual] contains subset [Expected]
	AssertContainsSubset
	AssertNotContainsSubset

	// Check expression: [Actual] belongs to list [Expected]
	// [Expected] stores AssertionList with allowed values
	AssertBelongs
	AssertNotBelongs
)

// AssertionSeverity defines how assertion failure should be treated.
type AssertionSeverity uint

//go:generate stringer -type=AssertionSeverity
const (
	// This assertion failure should mark current test as failed.
	// Typically handler will call t.Errorf().
	// This severity is used for most assertions.
	SeverityError AssertionSeverity = iota

	// This assertion failure is informational only, it can be logged,
	// but should not cause test failure.
	// Typically handler will call t.Logf(), or just ignore assertion.
	// This severity is used for assertions issued inside predicate functions,
	// e.g. in Array.Filter and Object.Filter.
	SeverityLog
)

// AssertionContext provides context where the assetion happened.
type AssertionContext struct {
	// Name of the running test
	// Usually comes from testing.T
	TestName string

	// Name of request being sent
	// Comes from Request.WithName()
	RequestName string

	// Chain of nested assertion names
	// Example value:
	//   {`Request("GET")`, `Expect()`, `JSON()`, `NotNull()`}
	Path []string

	// Chain of nested assertion names starting from alias
	// When alias is not set, AliasedPath has the same value as Path
	// Example value:
	//   {`foo`, `NotNull()`} // alias named foo
	AliasedPath []string

	// Request being sent
	// May be nil if request was not yet sent
	Request *Request

	// Response being matched
	// May be nil if response was not yet received
	Response *Response

	// Environment shared between tests
	// Comes from Expect instance
	Environment *Environment

	// Whether reporter is known to output to testing.TB
	// For example, true when reporter is testing.T or testify-based reporter.
	TestingTB bool
}

// AssertionFailure provides detailed information about failed assertion.
//
// [Type] and [Errors] fields are set for all assertions.
// [Actual], [Expected], [Reference], and [Delta] fields are set only for
// certain assertion types.
//
// The value itself is stored in [Actual.Value], [Expected.Value], etc.
// It allows to distinguish whether the value is not present at all,
// or is present but is nil.
//
// [Actual] stores the value being examined.
//
// Exact meaning of [Expected] depends on assertion type. It may be the value
// to which [Actual] was compared, or range to which [Actual] should belong,
// or pattern with which [Actual] should match, or element which [Actual]
// should contain, and so on.
//
// If [Reference] is set, it stores the value from which the check originated.
// For example, the user asked to check for unordered equality of arrays
// A and B. During comparison, a check failed that array A contains element E
// from array B. In this case [Actual] will be set to A (actually observed array),
// [Expected] will be set to E (expected but missing element), and [Reference]
// will be set to B (reference array that originated the check).
//
// If [Delta] is set, it stores maximum allowed difference between [Actual]
// and [Expected] values.
//
// For further details, see comments for corresponding AssertionType constant.
type AssertionFailure struct {
	// Type of failed assertion
	Type AssertionType

	// Severity of failure
	Severity AssertionSeverity

	// Deprecated: use Severity
	IsFatal bool

	// List of error messages
	Errors []error

	// Actually observed value
	Actual *AssertionValue

	// Expected value
	Expected *AssertionValue

	// Reference value
	Reference *AssertionValue

	// Allowed delta between actual and expected
	Delta *AssertionValue

	// Stacktrace of the failure
	Stacktrace []StacktraceEntry
}

// AssertionValue holds expected or actual value
type AssertionValue struct {
	Value interface{}
}

// AssertionRange holds inclusive range for allowed values
type AssertionRange struct {
	Min interface{}
	Max interface{}
}

// AssertionList holds list of allowed values
type AssertionList []interface{}

// AssertionHandler takes care of formatting and reporting test Failure or Success.
//
// You can log every performed assertion, or report only failures. You can implement
// custom formatting, for example, provide a JSON output for ulterior processing.
//
// Usually you don't need to implement AssertionHandler; instead you can implement
// Reporter, which is much simpler, and use it with DefaultAssertionHandler.
type AssertionHandler interface {
	// Invoked every time when an assertion succeeded.
	// May ignore failure, or log it, e.g. using t.Logf().
	Success(*AssertionContext)

	// Invoked every time when an assertion failed.
	// Handling depends on Failure.Severity field:
	//  - for SeverityError, reports failure to testing suite, e.g. using t.Errorf()
	//  - for SeverityLog, ignores failure, or logs it, e.g. using t.Logf()
	Failure(*AssertionContext, *AssertionFailure)
}

// DefaultAssertionHandler is default implementation for AssertionHandler.
//
//   - Formatter is used to format success and failure messages
//   - Reporter is used to report formatted fatal failure messages
//   - Logger is used to print formatted success and non-fatal failure messages
//
// Formatter and Reporter are required. Logger is optional.
// By default httpexpect creates DefaultAssertionHandler without Logger.
type DefaultAssertionHandler struct {
	Formatter Formatter
	Reporter  Reporter
	Logger    Logger
}

// Success implements AssertionHandler.Success.
func (h *DefaultAssertionHandler) Success(ctx *AssertionContext) {
	if h.Formatter == nil {
		panic("DefaultAssertionHandler.Formatter is nil")
	}

	if h.Logger == nil {
		return
	}

	msg := h.Formatter.FormatSuccess(ctx)

	h.Logger.Logf("%s", msg)
}

// Failure implements AssertionHandler.Failure.
func (h *DefaultAssertionHandler) Failure(
	ctx *AssertionContext, failure *AssertionFailure,
) {
	if h.Formatter == nil {
		panic("DefaultAssertionHandler.Formatter is nil")
	}

	switch failure.Severity {
	case SeverityError:
		if h.Reporter == nil {
			panic("DefaultAssertionHandler.Reporter is nil")
		}

		msg := h.Formatter.FormatFailure(ctx, failure)

		h.Reporter.Errorf("%s", msg)

	case SeverityLog:
		if h.Logger == nil {
			return
		}

		msg := h.Formatter.FormatFailure(ctx, failure)

		h.Logger.Logf("%s", msg)
	}
}
