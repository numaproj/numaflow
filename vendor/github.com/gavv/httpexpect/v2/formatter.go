package httpexpect

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/template"

	"github.com/TylerBrock/colorjson"
	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
	"github.com/mitchellh/go-wordwrap"
	"github.com/sanity-io/litter"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

// Formatter is used to format assertion messages into strings.
type Formatter interface {
	FormatSuccess(*AssertionContext) string
	FormatFailure(*AssertionContext, *AssertionFailure) string
}

// DefaultFormatter is the default Formatter implementation.
//
// DefaultFormatter gathers values from AssertionContext and AssertionFailure,
// converts them to strings, and creates FormatData struct. Then it passes
// FormatData to the template engine (text/template) to format message.
//
// You can control what is included and what is excluded from messages via
// several public fields.
//
// If desired, you can provide custom templates and function map. This may
// be easier than creating your own formatter from scratch.
type DefaultFormatter struct {
	// Exclude test name and request name from failure report.
	DisableNames bool

	// Exclude assertion path from failure report.
	DisablePaths bool

	// Exclude aliased assertion path from failure report.
	DisableAliases bool

	// Exclude diff from failure report.
	DisableDiffs bool

	// Exclude HTTP request from failure report.
	DisableRequests bool

	// Exclude HTTP response from failure report.
	DisableResponses bool

	// Thousand separator.
	// Default is DigitSeparatorUnderscore.
	DigitSeparator DigitSeparator

	// Float printing format.
	// Default is FloatFormatAuto.
	FloatFormat FloatFormat

	// Defines whether to print stacktrace on failure and in what format.
	// Default is StacktraceModeDisabled.
	StacktraceMode StacktraceMode

	// Colorization mode.
	// Default is ColorModeAuto.
	ColorMode ColorMode

	// Wrap text to keep lines below given width.
	// Use zero for default width, and negative value to disable wrapping.
	LineWidth int

	// If not empty, used to format success messages.
	// If empty, default template is used.
	SuccessTemplate string

	// If not empty, used to format failure messages.
	// If empty, default template is used.
	FailureTemplate string

	// When SuccessTemplate or FailureTemplate is set, this field
	// defines the function map passed to template engine.
	// May be nil.
	TemplateFuncs template.FuncMap
}

// FormatSuccess implements Formatter.FormatSuccess.
func (f *DefaultFormatter) FormatSuccess(ctx *AssertionContext) string {
	if f.SuccessTemplate != "" {
		return f.applyTemplate("SuccessTemplate",
			f.SuccessTemplate, f.TemplateFuncs, ctx, nil)
	} else {
		return f.applyTemplate("SuccessTemplate",
			defaultSuccessTemplate, defaultTemplateFuncs, ctx, nil)
	}
}

// FormatFailure implements Formatter.FormatFailure.
func (f *DefaultFormatter) FormatFailure(
	ctx *AssertionContext, failure *AssertionFailure,
) string {
	if f.FailureTemplate != "" {
		return f.applyTemplate("FailureTemplate",
			f.FailureTemplate, f.TemplateFuncs, ctx, failure)
	} else {
		return f.applyTemplate("FailureTemplate",
			defaultFailureTemplate, defaultTemplateFuncs, ctx, failure)
	}
}

// DigitSeparator defines the separator used to format integers and floats.
type DigitSeparator int

const (
	// Separate using underscore
	DigitSeparatorUnderscore DigitSeparator = iota

	// Separate using comma
	DigitSeparatorComma

	// Separate using apostrophe
	DigitSeparatorApostrophe

	// Do not separate
	DigitSeparatorNone
)

// FloatFormat defines the format in which all floats are printed.
type FloatFormat int

const (
	// Print floats in scientific notation for large exponents,
	// otherwise print in decimal notation.
	// Precision is the smallest needed to identify the value uniquely.
	// Similar to %g format.
	FloatFormatAuto FloatFormat = iota

	// Always print floats in decimal notation.
	// Precision is the smallest needed to identify the value uniquely.
	// Similar to %f format.
	FloatFormatDecimal

	// Always print floats in scientific notation.
	// Precision is the smallest needed to identify the value uniquely.
	// Similar to %e format.
	FloatFormatScientific
)

// StacktraceMode defines the format of stacktrace.
type StacktraceMode int

const (
	// Don't print stacktrace.
	StacktraceModeDisabled StacktraceMode = iota

	// Standard, verbose format.
	StacktraceModeStandard

	// Compact format.
	StacktraceModeCompact
)

// ColorMode defines how the text color is enabled.
type ColorMode int

const (
	// Automatically enable colors if ALL of the following is true:
	//  - stdout is a tty / console
	//  - AssertionHandler is known to output to testing.T
	//  - testing.Verbose() is true
	//
	// Colors are forcibly enabled if FORCE_COLOR environment variable
	// is set to a positive integer.
	//
	// Colors are forcibly disabled if TERM is "dumb" or NO_COLOR
	// environment variable is set to non-empty string.
	ColorModeAuto ColorMode = iota

	// Unconditionally enable colors.
	ColorModeAlways

	// Unconditionally disable colors.
	ColorModeNever
)

// FormatData defines data passed to template engine when DefaultFormatter
// formats assertion. You can use these fields in your custom templates.
type FormatData struct {
	TestName    string
	RequestName string

	AssertPath     []string
	AssertType     string
	AssertSeverity string

	Errors []string

	HaveActual bool
	Actual     string

	HaveExpected bool
	IsNegation   bool
	IsComparison bool
	ExpectedKind string
	Expected     []string

	HaveReference bool
	Reference     string

	HaveDelta bool
	Delta     string

	HaveDiff bool
	Diff     string

	HaveRequest bool
	Request     string

	HaveResponse bool
	Response     string

	HaveStacktrace bool
	Stacktrace     []string

	EnableColors bool
	LineWidth    int
}

const (
	kindRange      = "range"
	kindSchema     = "schema"
	kindPath       = "path"
	kindRegexp     = "regexp"
	kindFormat     = "format"
	kindFormatList = "formats"
	kindKey        = "key"
	kindElement    = "element"
	kindSubset     = "subset"
	kindValue      = "value"
	kindValueList  = "values"
)

func (f *DefaultFormatter) applyTemplate(
	templateName string,
	templateString string,
	templateFuncs template.FuncMap,
	ctx *AssertionContext,
	failure *AssertionFailure,
) string {
	templateData := f.buildFormatData(ctx, failure)

	t, err := template.New(templateName).Funcs(templateFuncs).Parse(templateString)
	if err != nil {
		panic(err)
	}

	var b bytes.Buffer

	err = t.Execute(&b, templateData)
	if err != nil {
		panic(err)
	}

	return b.String()
}

func (f *DefaultFormatter) buildFormatData(
	ctx *AssertionContext, failure *AssertionFailure,
) *FormatData {
	data := FormatData{}

	f.fillGeneral(&data, ctx)

	if failure != nil {
		data.AssertType = failure.Type.String()
		data.AssertSeverity = failure.Severity.String()

		f.fillErrors(&data, ctx, failure)

		if failure.Actual != nil {
			f.fillActual(&data, ctx, failure)
		}

		if failure.Expected != nil {
			f.fillExpected(&data, ctx, failure)
			f.fillIsNegation(&data, ctx, failure)
			f.fillIsComparison(&data, ctx, failure)
		}

		if failure.Reference != nil {
			f.fillReference(&data, ctx, failure)
		}

		if failure.Delta != nil {
			f.fillDelta(&data, ctx, failure)
		}

		f.fillRequest(&data, ctx, failure)
		f.fillResponse(&data, ctx, failure)
		f.fillStacktrace(&data, ctx, failure)
	}

	return &data
}

func (f *DefaultFormatter) fillGeneral(
	data *FormatData, ctx *AssertionContext,
) {
	if !f.DisableNames {
		data.TestName = ctx.TestName
		data.RequestName = ctx.RequestName
	}

	if !f.DisablePaths {
		if !f.DisableAliases {
			data.AssertPath = ctx.AliasedPath
		} else {
			data.AssertPath = ctx.Path
		}
	}

	switch f.ColorMode {
	case ColorModeAuto:
		switch colorMode() {
		case colorsUnsupported:
			data.EnableColors = false
		case colorsForced:
			data.EnableColors = true
		case colorsSupported:
			data.EnableColors = ctx.TestingTB && flag.Parsed() && testing.Verbose()
		}
	case ColorModeAlways:
		data.EnableColors = true
	case ColorModeNever:
		data.EnableColors = false
	}

	if f.LineWidth != 0 {
		data.LineWidth = f.LineWidth
	} else {
		data.LineWidth = defaultLineWidth
	}
}

func (f *DefaultFormatter) fillErrors(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	data.Errors = []string{}

	for _, err := range failure.Errors {
		if refIsNil(err) {
			continue
		}
		data.Errors = append(data.Errors, err.Error())
	}
}

func (f *DefaultFormatter) fillActual(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	switch failure.Type { //nolint
	case AssertUsage, AssertOperation:
		data.HaveActual = false

	case AssertType, AssertNotType:
		data.HaveActual = true
		data.Actual = f.formatTypedValue(failure.Actual.Value)

	default:
		data.HaveActual = true
		data.Actual = f.formatValue(failure.Actual.Value)
	}
}

func (f *DefaultFormatter) fillExpected(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	switch failure.Type {
	case AssertUsage, AssertOperation,
		AssertType, AssertNotType,
		AssertValid, AssertNotValid,
		AssertNil, AssertNotNil,
		AssertEmpty, AssertNotEmpty,
		AssertNotEqual:
		data.HaveExpected = false

	case AssertEqual:
		data.HaveExpected = true
		data.ExpectedKind = kindValue
		data.Expected = []string{
			f.formatValue(failure.Expected.Value),
		}

		if !f.DisableDiffs && failure.Actual != nil && failure.Expected != nil {
			data.Diff, data.HaveDiff = f.formatDiff(
				failure.Expected.Value, failure.Actual.Value)
		}

	case AssertLt, AssertLe, AssertGt, AssertGe:
		data.HaveExpected = true
		data.ExpectedKind = kindValue
		data.Expected = []string{
			f.formatValue(failure.Expected.Value),
		}

	case AssertInRange, AssertNotInRange:
		data.HaveExpected = true
		data.ExpectedKind = kindRange
		data.Expected = f.formatRangeValue(failure.Expected.Value)

	case AssertMatchSchema, AssertNotMatchSchema:
		data.HaveExpected = true
		data.ExpectedKind = kindSchema
		data.Expected = []string{
			f.formatMatchValue(failure.Expected.Value),
		}

	case AssertMatchPath, AssertNotMatchPath:
		data.HaveExpected = true
		data.ExpectedKind = kindPath
		data.Expected = []string{
			f.formatMatchValue(failure.Expected.Value),
		}

	case AssertMatchRegexp, AssertNotMatchRegexp:
		data.HaveExpected = true
		data.ExpectedKind = kindRegexp
		data.Expected = []string{
			f.formatMatchValue(failure.Expected.Value),
		}

	case AssertMatchFormat, AssertNotMatchFormat:
		data.HaveExpected = true
		if extractList(failure.Expected.Value) != nil {
			data.ExpectedKind = kindFormatList
		} else {
			data.ExpectedKind = kindFormat
		}
		data.Expected = f.formatListValue(failure.Expected.Value)

	case AssertContainsKey, AssertNotContainsKey:
		data.HaveExpected = true
		data.ExpectedKind = kindKey
		data.Expected = []string{
			f.formatValue(failure.Expected.Value),
		}

	case AssertContainsElement, AssertNotContainsElement:
		data.HaveExpected = true
		data.ExpectedKind = kindElement
		data.Expected = []string{
			f.formatValue(failure.Expected.Value),
		}

	case AssertContainsSubset, AssertNotContainsSubset:
		data.HaveExpected = true
		data.ExpectedKind = kindSubset
		data.Expected = []string{
			f.formatValue(failure.Expected.Value),
		}

	case AssertBelongs, AssertNotBelongs:
		data.HaveExpected = true
		data.ExpectedKind = kindValueList
		data.Expected = f.formatListValue(failure.Expected.Value)
	}
}

func (f *DefaultFormatter) fillIsNegation(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	switch failure.Type {
	case AssertUsage, AssertOperation,
		AssertType,
		AssertValid,
		AssertNil,
		AssertEmpty,
		AssertEqual,
		AssertLt, AssertLe, AssertGt, AssertGe,
		AssertInRange,
		AssertMatchSchema,
		AssertMatchPath,
		AssertMatchRegexp,
		AssertMatchFormat,
		AssertContainsKey,
		AssertContainsElement,
		AssertContainsSubset,
		AssertBelongs:
		break

	case AssertNotType,
		AssertNotValid,
		AssertNotNil,
		AssertNotEmpty,
		AssertNotEqual,
		AssertNotInRange,
		AssertNotMatchSchema,
		AssertNotMatchPath,
		AssertNotMatchRegexp,
		AssertNotMatchFormat,
		AssertNotContainsKey,
		AssertNotContainsElement,
		AssertNotContainsSubset,
		AssertNotBelongs:
		data.IsNegation = true
	}
}

func (f *DefaultFormatter) fillIsComparison(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	switch failure.Type { //nolint
	case AssertLt, AssertLe, AssertGt, AssertGe:
		data.IsComparison = true
	}
}

func (f *DefaultFormatter) fillReference(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	data.HaveReference = true
	data.Reference = f.formatValue(failure.Reference.Value)
}

func (f *DefaultFormatter) fillDelta(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	data.HaveDelta = true
	data.Delta = f.formatValue(failure.Delta.Value)
}

func (f *DefaultFormatter) fillRequest(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	if !f.DisableRequests && ctx.Request != nil && ctx.Request.httpReq != nil {
		dump, err := httputil.DumpRequest(ctx.Request.httpReq, false)
		if err != nil {
			return
		}

		data.HaveRequest = true
		data.Request = string(dump)
	}
}

func (f *DefaultFormatter) fillResponse(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	if !f.DisableResponses && ctx.Response != nil && ctx.Response.httpResp != nil {
		dump, err := httputil.DumpResponse(ctx.Response.httpResp, false)
		if err != nil {
			return
		}

		text := strings.Replace(string(dump), "\r\n", "\n", -1)
		lines := strings.SplitN(text, "\n", 2)

		data.HaveResponse = true
		data.Response = fmt.Sprintf("%s %s\n%s", lines[0], ctx.Response.rtt, lines[1])
	}
}

func (f *DefaultFormatter) fillStacktrace(
	data *FormatData, ctx *AssertionContext, failure *AssertionFailure,
) {
	data.Stacktrace = []string{}

	switch f.StacktraceMode {
	case StacktraceModeDisabled:
		break

	case StacktraceModeStandard:
		for _, entry := range failure.Stacktrace {
			data.HaveStacktrace = true
			data.Stacktrace = append(data.Stacktrace,
				fmt.Sprintf("%s()\n\t%s:%d +0x%x",
					entry.Func.Name(), entry.File, entry.Line, entry.FuncOffset))

		}

	case StacktraceModeCompact:
		for _, entry := range failure.Stacktrace {
			if entry.IsEntrypoint {
				break
			}
			data.HaveStacktrace = true
			data.Stacktrace = append(data.Stacktrace,
				fmt.Sprintf("%s() at %s:%d (%s)",
					entry.FuncName, filepath.Base(entry.File), entry.Line, entry.FuncPackage))

		}
	}
}

func (f *DefaultFormatter) formatValue(value interface{}) string {
	if flt := extractFloat32(value); flt != nil {
		return f.reformatNumber(f.formatFloatValue(*flt, 32))
	}

	if flt := extractFloat64(value); flt != nil {
		return f.reformatNumber(f.formatFloatValue(*flt, 64))
	}

	if refIsNum(value) {
		return f.reformatNumber(fmt.Sprintf("%v", value))
	}

	if !refIsNil(value) && !refIsHTTP(value) {
		if s, _ := value.(fmt.Stringer); s != nil {
			if ss := s.String(); strings.TrimSpace(ss) != "" {
				return ss
			}
		}
		if b, err := json.MarshalIndent(value, "", defaultIndent); err == nil {
			return string(b)
		}
	}

	sq := litter.Options{
		Separator: defaultIndent,
	}
	return sq.Sdump(value)
}

func (f *DefaultFormatter) formatFloatValue(value float64, bits int) string {
	switch f.FloatFormat {
	case FloatFormatAuto:
		if _, frac := math.Modf(value); frac != 0 {
			return strconv.FormatFloat(value, 'g', -1, bits)
		} else {
			return strconv.FormatFloat(value, 'f', -1, bits)
		}

	case FloatFormatDecimal:
		return strconv.FormatFloat(value, 'f', -1, bits)

	case FloatFormatScientific:
		return strconv.FormatFloat(value, 'e', -1, bits)

	default:
		return fmt.Sprintf("%v", value)
	}
}

func (f *DefaultFormatter) formatTypedValue(value interface{}) string {
	if refIsNum(value) {
		return fmt.Sprintf("%T(%v)", value, f.formatValue(value))
	}

	return fmt.Sprintf("%T(%#v)", value, value)
}

func (f *DefaultFormatter) formatMatchValue(value interface{}) string {
	if str := extractString(value); str != nil {
		return *str
	}

	return f.formatValue(value)
}

func (f *DefaultFormatter) formatRangeValue(value interface{}) []string {
	if rng := exctractRange(value); rng != nil {
		if refIsNum(rng.Min) && refIsNum(rng.Max) {
			return []string{
				fmt.Sprintf("[%v; %v]", f.formatValue(rng.Min), f.formatValue(rng.Max)),
			}
		} else {
			return []string{
				fmt.Sprintf("%v", rng.Min),
				fmt.Sprintf("%v", rng.Max),
			}
		}
	} else {
		return []string{
			f.formatValue(value),
		}
	}
}

func (f *DefaultFormatter) formatListValue(value interface{}) []string {
	if lst := extractList(value); lst != nil {
		s := make([]string, 0, len(*lst))
		for _, e := range *lst {
			s = append(s, f.formatValue(e))
		}
		return s
	} else {
		return []string{
			f.formatValue(value),
		}
	}
}

func (f *DefaultFormatter) formatDiff(expected, actual interface{}) (string, bool) {
	differ := gojsondiff.New()

	var diff gojsondiff.Diff

	if ve, ok := expected.(map[string]interface{}); ok {
		if va, ok := actual.(map[string]interface{}); ok {
			diff = differ.CompareObjects(ve, va)
		} else {
			return "", false
		}
	} else if ve, ok := expected.([]interface{}); ok {
		if va, ok := actual.([]interface{}); ok {
			diff = differ.CompareArrays(ve, va)
		} else {
			return "", false
		}
	} else {
		return "", false
	}

	if !diff.Modified() {
		return "", false
	}

	config := formatter.AsciiFormatterConfig{
		ShowArrayIndex: true,
	}
	fa := formatter.NewAsciiFormatter(expected, config)

	str, err := fa.Format(diff)
	if err != nil {
		return "", false
	}

	diffText := "--- expected\n+++ actual\n" + str

	return diffText, true
}

func (f *DefaultFormatter) reformatNumber(numStr string) string {
	signPart, intPart, fracPart, expPart := f.decomposeNumber(numStr)
	if intPart == "" {
		return numStr
	}

	var sb strings.Builder

	sb.WriteString(signPart)
	sb.WriteString(f.applySeparator(intPart, -1))

	if fracPart != "" {
		sb.WriteString(".")
		sb.WriteString(f.applySeparator(fracPart, +1))
	}

	if expPart != "" {
		sb.WriteString("e")
		sb.WriteString(expPart)
	}

	return sb.String()
}

var (
	decomposeRegexp = regexp.MustCompile(`^([+-])?(\d+)([.](\d+))?([eE]([+-]?\d+))?$`)
)

func (f *DefaultFormatter) decomposeNumber(numStr string) (
	signPart, intPart, fracPart, expPart string,
) {
	parts := decomposeRegexp.FindStringSubmatch(numStr)

	if len(parts) > 1 {
		signPart = parts[1]
	}
	if len(parts) > 2 {
		intPart = parts[2]
	}
	if len(parts) > 4 {
		fracPart = parts[4]
	}
	if len(parts) > 6 {
		expPart = parts[6]
	}

	return
}

func (f *DefaultFormatter) applySeparator(numStr string, dir int) string {
	var separator string
	switch f.DigitSeparator {
	case DigitSeparatorUnderscore:
		separator = "_"
		break
	case DigitSeparatorApostrophe:
		separator = "'"
		break
	case DigitSeparatorComma:
		separator = ","
		break
	case DigitSeparatorNone:
	default:
		return numStr
	}

	var sb strings.Builder

	cnt := 0
	if dir < 0 {
		cnt = len(numStr)
	}

	for i := 0; i != len(numStr); i++ {
		sb.WriteByte(numStr[i])

		cnt += dir
		if cnt%3 == 0 && i != len(numStr)-1 {
			sb.WriteString(separator)
		}
	}

	return sb.String()
}

func extractString(value interface{}) *string {
	switch s := value.(type) {
	case string:
		return &s
	default:
		return nil
	}
}

func extractFloat32(value interface{}) *float64 {
	switch f := value.(type) {
	case float32:
		ff := float64(f)
		return &ff
	default:
		return nil
	}
}

func extractFloat64(value interface{}) *float64 {
	switch f := value.(type) {
	case float64:
		return &f
	default:
		return nil
	}
}

func exctractRange(value interface{}) *AssertionRange {
	switch rng := value.(type) {
	case AssertionRange:
		return &rng
	case *AssertionRange: // invalid, but we handle it
		return rng
	default:
		return nil
	}
}

func extractList(value interface{}) *AssertionList {
	switch lst := value.(type) {
	case AssertionList:
		return &lst
	case *AssertionList: // invalid, but we handle it
		return lst
	default:
		return nil
	}
}

var (
	colorsSupportedOnce sync.Once
	colorsSupportedMode int
)

const (
	colorsUnsupported = iota
	colorsSupported
	colorsForced
)

func colorMode() int {
	colorsSupportedOnce.Do(func() {
		if s := os.Getenv("FORCE_COLOR"); len(s) != 0 {
			if n, err := strconv.Atoi(s); err == nil && n > 0 {
				colorsSupportedMode = colorsForced
				return
			}
		}

		if (isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())) &&
			len(os.Getenv("NO_COLOR")) == 0 &&
			!strings.HasPrefix(os.Getenv("TERM"), "dumb") {
			colorsSupportedMode = colorsSupported
			return
		}

		colorsSupportedMode = colorsUnsupported
		return
	})

	return colorsSupportedMode
}

const (
	defaultIndent    = "  "
	defaultLineWidth = 60
)

var defaultColors = map[string]color.Attribute{
	// regular
	"Black":   color.FgBlack,
	"Red":     color.FgRed,
	"Green":   color.FgGreen,
	"Yellow":  color.FgYellow,
	"Magenta": color.FgMagenta,
	"Cyan":    color.FgCyan,
	"White":   color.FgWhite,
	// bright
	"HiBlack":   color.FgHiBlack,
	"HiRed":     color.FgHiRed,
	"HiGreen":   color.FgHiGreen,
	"HiYellow":  color.FgHiYellow,
	"HiMagenta": color.FgHiMagenta,
	"HiCyan":    color.FgHiCyan,
	"HiWhite":   color.FgHiWhite,
}

var defaultTemplateFuncs = template.FuncMap{
	"trim": func(input string) string {
		return strings.TrimSpace(input)
	},
	"indent": func(input string) string {
		var sb strings.Builder

		for _, s := range strings.Split(input, "\n") {
			if sb.Len() != 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(defaultIndent)
			sb.WriteString(s)
		}

		return sb.String()
	},
	"wrap": func(width int, input string) string {
		input = strings.TrimSpace(input)

		width -= len(defaultIndent)
		if width <= 0 {
			return input
		}

		return wordwrap.WrapString(input, uint(width))
	},
	"join": func(width int, tokenList []string) string {
		width -= len(defaultIndent)
		if width <= 0 {
			return strings.Join(tokenList, ".")
		}

		var sb strings.Builder

		lineLen := 0
		lineNum := 0

		write := func(s string) {
			sb.WriteString(s)
			lineLen += len(s)
		}

		for n, token := range tokenList {
			if lineLen+len(token)+1 > width {
				write("\n")
				lineLen = 0
				if lineNum < 2 {
					lineNum++
				}
			}
			if lineLen == 0 {
				for l := 0; l < lineNum; l++ {
					write(defaultIndent)
				}
			}
			write(token)
			if n != len(tokenList)-1 {
				write(".")
			}
		}

		return sb.String()
	},
	"color": func(enable bool, colorName, input string) string {
		if !enable {
			return input
		}
		colorAttr := color.Reset
		if ca, ok := defaultColors[colorName]; ok {
			colorAttr = ca
		}
		return color.New(colorAttr).Sprint(input)
	},
	"colorhttp": func(enable bool, isResponse bool, input string) string {
		if !enable {
			return input
		}

		methodColor := color.New(defaultColors["HiMagenta"])
		statusColor := color.New(defaultColors["HiMagenta"])
		headerColor := color.New(defaultColors["Cyan"])

		var sb strings.Builder

		isFirstLine := true
		for _, line := range strings.Split(input, "\n") {
			if sb.Len() != 0 {
				sb.WriteString("\n")
			}

			line = strings.TrimSuffix(line, "\n")
			line = strings.TrimSuffix(line, "\r")

			var words []string
			if isFirstLine {
				words = strings.SplitN(line, " ", -1)
			} else {
				words = strings.SplitN(line, " ", 2)
			}

			wordLen := len(words)
			for index, word := range words {
				var applyColor *color.Color

				if isFirstLine {
					if isResponse {
						if index != 0 && index != wordLen-1 {
							applyColor = statusColor
						}
					} else {
						if index == 0 {
							applyColor = methodColor
						}
					}
				} else {
					if index == 0 {
						applyColor = headerColor
					}
				}

				if word != "" && applyColor != nil {
					sb.WriteString(applyColor.Sprint(word))
				} else {
					sb.WriteString(word)
				}

				sb.WriteString(" ")
			}

			isFirstLine = false
		}

		return sb.String()
	},
	"colorjson": func(enable bool, colorName, input string) string {
		if !enable {
			return input
		}

		fallbackColor := color.Reset
		if attr, ok := defaultColors[colorName]; ok {
			fallbackColor = attr
		}

		var parsedInput interface{}
		err := json.Unmarshal([]byte(input), &parsedInput)
		if err != nil {
			return color.New(fallbackColor).Sprint(input)
		}

		formatter := colorjson.NewFormatter()
		formatter.KeyColor = color.New(color.Reset)
		formatter.StringColor = color.New(defaultColors["HiMagenta"])
		formatter.NumberColor = color.New(defaultColors["Cyan"])
		formatter.BoolColor = color.New(defaultColors["Cyan"])
		formatter.NullColor = color.New(defaultColors["Cyan"])
		formatter.Indent = 2

		b, err := formatter.Marshal(parsedInput)
		if err != nil {
			return color.New(fallbackColor).Sprint(input)
		}

		return string(b)
	},
	"colordiff": func(enable bool, input string) string {
		if !enable {
			return input
		}

		prefixColor := [...]struct {
			prefix string
			color  color.Attribute
		}{
			{"---", color.FgWhite},
			{"+++", color.FgWhite},
			{"-", color.FgRed},
			{"+", color.FgGreen},
		}

		lineColor := func(s string) color.Attribute {
			for _, pc := range prefixColor {
				if strings.HasPrefix(s, pc.prefix) {
					return pc.color
				}
			}

			return color.Reset
		}

		var sb strings.Builder
		for _, line := range strings.Split(input, "\n") {
			if sb.Len() != 0 {
				sb.WriteString("\n")
			}

			sb.WriteString(color.New(lineColor(line)).Sprint(line))
		}

		return sb.String()
	},
}

var defaultSuccessTemplate = `[OK] {{ join .LineWidth .AssertPath }}`

var defaultFailureTemplate = `
{{- range $n, $err := .Errors }}
{{ if eq $n 0 -}}
{{ $err | wrap $.LineWidth | color $.EnableColors "Red" }}
{{- else -}}
{{ $err | wrap $.LineWidth | indent | color $.EnableColors "Red" }}
{{- end -}}
{{- end -}}
{{- if .TestName }}

test name: {{ .TestName | color $.EnableColors "Cyan" }}
{{- end -}}
{{- if .RequestName }}

request name: {{ .RequestName | color $.EnableColors "Cyan" }}
{{- end -}}
{{- if .HaveRequest }}

request: {{ .Request | colorhttp $.EnableColors false | indent | trim }}
{{- end -}}
{{- if .HaveResponse }}

response: {{ .Response | colorhttp $.EnableColors true | indent | trim }}
{{- end -}}
{{- if .HaveStacktrace }}

trace:
{{- range $n, $call := .Stacktrace }}
{{ $call | indent }}
{{- end -}}
{{- end -}}
{{- if .AssertPath }}

assertion:
{{ join .LineWidth .AssertPath | indent | color .EnableColors "Yellow" }}
{{- end -}}
{{- if .HaveExpected }}

{{ if .IsNegation }}denied
{{- else if .IsComparison }}compared
{{- else }}expected
{{- end }} {{ .ExpectedKind }}:
{{- range $n, $exp := .Expected }}
{{ $exp | colorjson $.EnableColors "HiMagenta" | indent }}
{{- end -}}
{{- end -}}
{{- if .HaveActual }}

actual value:
{{ .Actual | colorjson .EnableColors "HiMagenta" | indent }}
{{- end -}}
{{- if .HaveReference }}

reference value:
{{ .Reference | colorjson .EnableColors "HiMagenta" | indent }}
{{- end -}}
{{- if .HaveDelta }}

allowed delta:
{{ .Delta | indent | color .EnableColors "Cyan" }}
{{- end -}}
{{- if .HaveDiff }}

diff:
{{ .Diff | colordiff .EnableColors | indent }}
{{- end -}}
`
