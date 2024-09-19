package httpexpect

import (
	"errors"
	"fmt"
)

func validateAssertion(failure *AssertionFailure) error {
	if len(failure.Errors) == 0 {
		return errors.New("AssertionFailure should have non-empty Errors list")
	}

	for _, err := range failure.Errors {
		if err == nil {
			return errors.New("AssertionFailure should not have nil in Errors")
		} else if refIsNil(err) {
			return errors.New("AssertionFailure should not have typed-nil in Errors")
		}
	}

	if err := validateType(failure); err != nil {
		return err
	}

	return nil
}

func validateType(failure *AssertionFailure) error {
	switch failure.Type {
	case AssertUsage, AssertOperation:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldDenied,
			Expected: fieldDenied,
		})

	case AssertType, AssertNotType,
		AssertValid, AssertNotValid,
		AssertNil, AssertNotNil,
		AssertEmpty, AssertNotEmpty:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldRequired,
			Expected: fieldDenied,
		})

	case AssertEqual, AssertNotEqual,
		AssertLt, AssertLe, AssertGt, AssertGe:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldRequired,
			Expected: fieldRequired,
		})

	case AssertInRange, AssertNotInRange:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldRequired,
			Expected: fieldRequired,
			Range:    fieldRequired,
		})

	case AssertMatchSchema, AssertNotMatchSchema,
		AssertMatchPath, AssertNotMatchPath,
		AssertMatchRegexp, AssertNotMatchRegexp,
		AssertMatchFormat, AssertNotMatchFormat:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldRequired,
			Expected: fieldRequired,
		})

	case AssertContainsKey, AssertNotContainsKey,
		AssertContainsElement, AssertNotContainsElement,
		AssertContainsSubset, AssertNotContainsSubset:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldRequired,
			Expected: fieldOptional,
		})

	case AssertBelongs, AssertNotBelongs:
		return validateTraits(failure, fieldTraits{
			Actual:   fieldRequired,
			Expected: fieldRequired,
			List:     fieldRequired,
		})
	}

	return fmt.Errorf("unknown assertion type %s", failure.Type)
}

type fieldRequirement uint

const (
	fieldOptional fieldRequirement = iota
	fieldRequired
	fieldDenied
)

type fieldTraits struct {
	Actual   fieldRequirement
	Expected fieldRequirement
	Range    fieldRequirement
	List     fieldRequirement
}

func validateTraits(failure *AssertionFailure, traits fieldTraits) error {
	switch traits.Actual {
	case fieldRequired:
		if failure.Actual == nil {
			return fmt.Errorf(
				"AssertionFailure of type %s should have Actual field",
				failure.Type)
		}

	case fieldDenied:
		if failure.Actual != nil {
			return fmt.Errorf(
				"AssertionFailure of type %s can't have Actual field",
				failure.Type)
		}

	case fieldOptional:
		break
	}

	switch traits.Expected {
	case fieldRequired:
		if failure.Expected == nil {
			return fmt.Errorf(
				"AssertionFailure of type %s should have Expected field",
				failure.Type)
		}

	case fieldDenied:
		if failure.Expected != nil {
			return fmt.Errorf(
				"AssertionFailure of type %s can't have Expected field",
				failure.Type)
		}

	case fieldOptional:
		break
	}

	if traits.Range == fieldRequired || traits.Range == fieldOptional {
		if failure.Expected != nil {
			if _, ok := failure.Expected.Value.(*AssertionRange); ok {
				return errors.New(
					"AssertionValue should contain AssertionRange, not *AssertionRange")
			}

			if rng, ok := failure.Expected.Value.(AssertionRange); ok {
				if rng.Min == nil {
					return errors.New("AssertionRange value should have non-nil Min field")
				}
				if rng.Max == nil {
					return errors.New("AssertionRange value should have non-nil Max field")
				}
			}
		}
	}

	switch traits.Range {
	case fieldRequired:
		if failure.Expected == nil {
			return fmt.Errorf(
				"AssertionFailure of type %s should have Expected field",
				failure.Type)
		}

		if _, ok := failure.Expected.Value.(AssertionRange); !ok {
			return fmt.Errorf(
				"AssertionFailure of type %s"+
					" should have Expected field with AssertionRange value",
				failure.Type)
		}

	case fieldDenied:
		panic("unsupported")

	case fieldOptional:
		break
	}

	if traits.List == fieldRequired || traits.List == fieldOptional {
		if failure.Expected != nil {
			if _, ok := failure.Expected.Value.(*AssertionList); ok {
				return errors.New(
					"AssertionValue should contain AssertionList, not *AssertionList")
			}

			if lst, ok := failure.Expected.Value.(AssertionList); ok {
				if len(lst) == 0 {
					return errors.New("AssertionList should be non-empty")
				}
			}
		}
	}

	switch traits.List {
	case fieldRequired:
		if failure.Expected == nil {
			return fmt.Errorf(
				"AssertionFailure of type %s should have Expected field",
				failure.Type)
		}

		if _, ok := failure.Expected.Value.(AssertionList); !ok {
			return fmt.Errorf(
				"AssertionFailure of type %s"+
					" should have Expected field with AssertionList value",
				failure.Type)
		}

	case fieldDenied:
		panic("unsupported")

	case fieldOptional:
		break
	}

	return nil
}
