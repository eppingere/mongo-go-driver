package assert

import (
	"reflect"
)

// Tester is an interface wrapper for types that allow failing tests.
type Tester interface {
	Fatalf(string, ...interface{})
}

// Helper is an interface wrapper for types that allow declaring functions as test helpers.
type Helper interface {
	Helper()
}

// Equal compares expected and actual for equality.
// msgAndArgs: Message string and format arguments to print if expected and actual are not equal. The first argument
// must be a string.
// If nothing is given for msgAndArgs, a default message of "expected <expected> but received <actual>" will be printed.
func Equal(t Tester, expected, actual interface{}, msgAndArgs ...interface{}) {
	if h, ok := t.(Helper); ok {
		h.Helper()
	}

	if reflect.DeepEqual(expected, actual) {
		return
	}

	msg, args := separateMsgAndArgs(msgAndArgs...)
	if msg == "" {
		msg = "expected %v but received %v"
		args = []interface{}{expected, actual}
	}
	t.Fatalf(msg, args...)
}

// True asserts that the actual parameter is true.
func True(t Tester, actual interface{}, msgAndArgs ...interface{}) {
	if h, ok := t.(Helper); ok {
		h.Helper()
	}

	Equal(t, true, actual, msgAndArgs...)
}

// False asserts that the actual parameter is true.
func False(t Tester, actual interface{}, msgAndArgs ...interface{}) {
	if h, ok := t.(Helper); ok {
		h.Helper()
	}

	Equal(t, false, actual, msgAndArgs...)
}

// Nil asserts that the actual parameter is nil.
func Nil(t Tester, actual interface{}, msgAndArgs ...interface{}) {
	if h, ok := t.(Helper); ok {
		h.Helper()
	}

	if isNil(actual) {
		return
	}
	msg, args := separateMsgAndArgs(msgAndArgs...)
	if msg == "" {
		msg = "expected %v to be nil but received non-nil"
		args = []interface{}{actual}
	}
	t.Fatalf(msg, args...)
}

// NotNil asserts that the actual parameter is not nil.
func NotNil(t Tester, actual interface{}, msgAndArgs ...interface{}) {
	if h, ok := t.(Helper); ok {
		h.Helper()
	}

	if !isNil(actual) {
		return
	}
	msg, args := separateMsgAndArgs(msgAndArgs...)
	if msg == "" {
		msg = "expected %v to be non-nil but received nil"
	}
	t.Fatalf(msg, args...)
}

func separateMsgAndArgs(msgAndArgs ...interface{}) (string, []interface{}) {
	if len(msgAndArgs) == 0 {
		return "", nil
	}

	msg := msgAndArgs[0].(string)
	var args []interface{}
	if len(msgAndArgs) > 1 {
		args = msgAndArgs[1:]
	}
	return msg, args
}

func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	val := reflect.ValueOf(object)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return val.IsNil()
	default:
		return false
	}
}
