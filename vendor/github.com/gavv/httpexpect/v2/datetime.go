package httpexpect

import (
	"errors"
	"time"
)

// DateTime provides methods to inspect attached time.Time value.
type DateTime struct {
	noCopy noCopy
	chain  *chain
	value  time.Time
}

// NewDateTime returns a new DateTime instance.
//
// If reporter is nil, the function panics.
//
// Example:
//
//	dt := NewDateTime(t, time.Now())
//	dt.Le(time.Now())
//
//	time.Sleep(time.Second)
//	dt.Lt(time.Now())
func NewDateTime(reporter Reporter, value time.Time) *DateTime {
	return newDateTime(newChainWithDefaults("DateTime()", reporter), value)
}

// NewDateTimeC returns a new DateTime instance with config.
//
// Requirements for config are same as for WithConfig function.
//
// See NewDateTime for usage example.
func NewDateTimeC(config Config, value time.Time) *DateTime {
	return newDateTime(newChainWithConfig("DateTime()", config.withDefaults()), value)
}

func newDateTime(parent *chain, val time.Time) *DateTime {
	return &DateTime{chain: parent.clone(), value: val}
}

// Raw returns underlying time.Time value attached to DateTime.
// This is the value originally passed to NewDateTime.
//
// Example:
//
//	dt := NewDateTime(t, timestamp)
//	assert.Equal(t, timestamp, dt.Raw())
func (dt *DateTime) Raw() time.Time {
	return dt.value
}

// Alias is similar to Value.Alias.
func (dt *DateTime) Alias(name string) *DateTime {
	opChain := dt.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	dt.chain.setAlias(name)
	return dt
}

// Zone returns a new String instance with datetime zone.
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Zone().IsEqual("IST")
func (dt *DateTime) Zone() *String {
	opChain := dt.chain.enter("Zone()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	zone, _ := dt.value.Zone()
	return newString(opChain, zone)
}

// Year returns the year in which datetime occurs,
// in the range [0, 9999]
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Year().IsEqual(2022)
func (dt *DateTime) Year() *Number {
	opChain := dt.chain.enter("Year()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Year()))
}

// Month returns the month of the year specified by datetime,
// in the range [1,12].
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Month().IsEqual(12)
func (dt *DateTime) Month() *Number {
	opChain := dt.chain.enter("Month()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Month()))
}

// Day returns the day of the month specified datetime,
// in the range [1,31].
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Day().IsEqual(30)
func (dt *DateTime) Day() *Number {
	opChain := dt.chain.enter("Day()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Day()))
}

// Weekday returns the day of the week specified by datetime,
// in the range [0, 6], 0 corresponds to Sunday
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.WeekDay().IsEqual(time.Friday)
func (dt *DateTime) WeekDay() *Number {
	opChain := dt.chain.enter("WeekDay()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Weekday()))
}

// YearDay returns the day of the year specified by datetime,
// in the range [1,365] for non-leap years,
// and [1,366] in leap years.
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.YearDay().IsEqual(364)
func (dt *DateTime) YearDay() *Number {
	opChain := dt.chain.enter("YearDay()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.YearDay()))
}

// Hour returns the hour within the day specified by datetime,
// in the range [0, 23].
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Hour().IsEqual(15)
func (dt *DateTime) Hour() *Number {
	opChain := dt.chain.enter("Hour()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Hour()))
}

// Minute returns the minute offset within the hour specified by datetime,
// in the range [0, 59].
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Minute().IsEqual(4)
func (dt *DateTime) Minute() *Number {
	opChain := dt.chain.enter("Minute()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Minute()))
}

// Second returns the second offset within the minute specified by datetime,
// in the range [0, 59].
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Second().IsEqual(5)
func (dt *DateTime) Second() *Number {
	opChain := dt.chain.enter("Second()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Second()))
}

// Nanosecond returns the nanosecond offset within the second specified by datetime,
// in the range [0, 999999999].
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.Nanosecond().IsEqual(0)
func (dt *DateTime) Nanosecond() *Number {
	opChain := dt.chain.enter("Nanosecond()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, float64(0))
	}

	return newNumber(opChain, float64(dt.value.Nanosecond()))
}

// Deprecated: use Zone instead.
func (dt *DateTime) GetZone() *String {
	return dt.Zone()
}

// Deprecated: use Year instead.
func (dt *DateTime) GetYear() *Number {
	return dt.Year()
}

// Deprecated: use Month instead.
func (dt *DateTime) GetMonth() *Number {
	return dt.Month()
}

// Deprecated: use Day instead.
func (dt *DateTime) GetDay() *Number {
	return dt.Day()
}

// Deprecated: use WeekDay instead.
func (dt *DateTime) GetWeekDay() *Number {
	return dt.WeekDay()
}

// Deprecated: use YearDay instead.
func (dt *DateTime) GetYearDay() *Number {
	return dt.YearDay()
}

// Deprecated: use Hour instead.
func (dt *DateTime) GetHour() *Number {
	return dt.Hour()
}

// Deprecated: use Minute instead.
func (dt *DateTime) GetMinute() *Number {
	return dt.Minute()
}

// Deprecated: use Second instead.
func (dt *DateTime) GetSecond() *Number {
	return dt.Second()
}

// Deprecated: use Nanosecond instead.
func (dt *DateTime) GetNanosecond() *Number {
	return dt.Nanosecond()
}

// IsEqual succeeds if DateTime is equal to given value.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 1))
//	dt.IsEqual(time.Unix(0, 1))
func (dt *DateTime) IsEqual(value time.Time) *DateTime {
	opChain := dt.chain.enter("IsEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if !dt.value.Equal(value) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time points are equal"),
			},
		})
	}

	return dt
}

// NotEqual succeeds if DateTime is not equal to given value.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 1))
//	dt.NotEqual(time.Unix(0, 2))
func (dt *DateTime) NotEqual(value time.Time) *DateTime {
	opChain := dt.chain.enter("NotEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if dt.value.Equal(value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time points are non-equal"),
			},
		})
	}

	return dt
}

// Deprecated: use IsEqual instead.
func (dt *DateTime) Equal(value time.Time) *DateTime {
	return dt.IsEqual(value)
}

// InRange succeeds if DateTime is within given range [min; max].
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 2))
//	dt.InRange(time.Unix(0, 1), time.Unix(0, 3))
//	dt.InRange(time.Unix(0, 2), time.Unix(0, 2))
func (dt *DateTime) InRange(min, max time.Time) *DateTime {
	opChain := dt.chain.enter("InRange()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if !((dt.value.After(min) || dt.value.Equal(min)) &&
		(dt.value.Before(max) || dt.value.Equal(max))) {
		opChain.fail(AssertionFailure{
			Type:     AssertInRange,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{AssertionRange{min, max}},
			Errors: []error{
				errors.New("expected: time point is within given range"),
			},
		})
	}

	return dt
}

// NotInRange succeeds if DateTime is not within given range [min; max].
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 10))
//	dt.NotInRange(time.Unix(0, 1), time.Unix(0, 9))
//	dt.NotInRange(time.Unix(0, 11), time.Unix(0, 20))
func (dt *DateTime) NotInRange(min, max time.Time) *DateTime {
	opChain := dt.chain.enter("NotInRange()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if (dt.value.After(min) || dt.value.Equal(min)) &&
		(dt.value.Before(max) || dt.value.Equal(max)) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotInRange,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{AssertionRange{min, max}},
			Errors: []error{
				errors.New("expected: time point is not within given range"),
			},
		})
	}

	return dt
}

// InList succeeds if DateTime is equal to one of the values from given
// list of time.Time.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 2))
//	dt.InRange(time.Unix(0, 1), time.Unix(0, 2))
func (dt *DateTime) InList(values ...time.Time) *DateTime {
	opChain := dt.chain.enter("InList()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return dt
	}

	var isListed bool
	for _, v := range values {
		if dt.value.Equal(v) {
			isListed = true
			break
		}
	}

	if !isListed {
		valueList := make([]interface{}, 0, len(values))
		for _, v := range values {
			valueList = append(valueList, v)
		}

		opChain.fail(AssertionFailure{
			Type:     AssertBelongs,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{AssertionList(valueList)},
			Errors: []error{
				errors.New("expected: time point is equal to one of the values"),
			},
		})
	}

	return dt
}

// NotInList succeeds if DateTime is not equal to any of the values from
// given list of time.Time.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 2))
//	dt.InRange(time.Unix(0, 1), time.Unix(0, 3))
func (dt *DateTime) NotInList(values ...time.Time) *DateTime {
	opChain := dt.chain.enter("NotInList()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return dt
	}

	for _, v := range values {
		if dt.value.Equal(v) {
			valueList := make([]interface{}, 0, len(values))
			for _, v := range values {
				valueList = append(valueList, v)
			}

			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{dt.value},
				Expected: &AssertionValue{AssertionList(valueList)},
				Errors: []error{
					errors.New("expected: time point is not equal to any of the values"),
				},
			})

			return dt
		}
	}

	return dt
}

// Gt succeeds if DateTime is greater than given value.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 2))
//	dt.Gt(time.Unix(0, 1))
func (dt *DateTime) Gt(value time.Time) *DateTime {
	opChain := dt.chain.enter("Gt()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if !dt.value.After(value) {
		opChain.fail(AssertionFailure{
			Type:     AssertGt,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time point is after given time"),
			},
		})
	}

	return dt
}

// Ge succeeds if DateTime is greater than or equal to given value.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 2))
//	dt.Ge(time.Unix(0, 1))
func (dt *DateTime) Ge(value time.Time) *DateTime {
	opChain := dt.chain.enter("Ge()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if !(dt.value.After(value) || dt.value.Equal(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertGe,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time point is after or equal to given time"),
			},
		})
	}

	return dt
}

// Lt succeeds if DateTime is lesser than given value.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 1))
//	dt.Lt(time.Unix(0, 2))
func (dt *DateTime) Lt(value time.Time) *DateTime {
	opChain := dt.chain.enter("Lt()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if !dt.value.Before(value) {
		opChain.fail(AssertionFailure{
			Type:     AssertLt,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time point is before given time"),
			},
		})
	}

	return dt
}

// Le succeeds if DateTime is lesser than or equal to given value.
//
// Example:
//
//	dt := NewDateTime(t, time.Unix(0, 1))
//	dt.Le(time.Unix(0, 2))
func (dt *DateTime) Le(value time.Time) *DateTime {
	opChain := dt.chain.enter("Le()")
	defer opChain.leave()

	if opChain.failed() {
		return dt
	}

	if !(dt.value.Before(value) || dt.value.Equal(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertLe,
			Actual:   &AssertionValue{dt.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time point is before or equal to given time"),
			},
		})
	}

	return dt
}

// AsUTC returns a new DateTime instance in UTC timeZone.
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.AsUTC().Zone().IsEqual("UTC")
func (dt *DateTime) AsUTC() *DateTime {
	opChain := dt.chain.enter("AsUTC()")
	defer opChain.leave()

	if opChain.failed() {
		return newDateTime(opChain, time.Unix(0, 0))
	}

	return newDateTime(opChain, dt.value.UTC())
}

// AsLocal returns a new DateTime instance in Local timeZone.
//
// Example:
//
//	tm, _ := time.Parse(time.UnixDate, "Fri Dec 30 15:04:05 IST 2022")
//	dt := NewDateTime(t, tm)
//	dt.AsLocal().Zone().IsEqual("IST")
func (dt *DateTime) AsLocal() *DateTime {
	opChain := dt.chain.enter("AsLocal()")
	defer opChain.leave()

	if opChain.failed() {
		return newDateTime(opChain, time.Unix(0, 0))
	}

	return newDateTime(opChain, dt.value.Local())
}
