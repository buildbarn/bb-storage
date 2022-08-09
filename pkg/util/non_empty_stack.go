package util

// NonEmptyStack is a stack data type that, if initialized properly, is
// guaranteed to remain non-empty.
type NonEmptyStack[T any] struct {
	stack []T
}

// NewNonEmptyStack returns a non-empty stack that is initialized with a
// single element.
func NewNonEmptyStack[T any](base T) NonEmptyStack[T] {
	return NonEmptyStack[T]{
		stack: []T{base},
	}
}

// Copy all of the elements in a non-empty stack, returning a new
// instance.
func (cw *NonEmptyStack[T]) Copy() NonEmptyStack[T] {
	return NonEmptyStack[T]{
		stack: append([]T(nil), cw.stack...),
	}
}

// Peek at the element that was last pushed into the stack.
func (cw *NonEmptyStack[T]) Peek() T {
	return cw.stack[len(cw.stack)-1]
}

// Push a new element on top of the stack.
func (cw *NonEmptyStack[T]) Push(d T) {
	cw.stack = append(cw.stack, d)
}

// PopSingle removes the last pushed element from the stack. The return
// value indicates whether an element was popped successfully. It is not
// possible to push the final element off the stack.
func (cw *NonEmptyStack[T]) PopSingle() (T, bool) {
	if len(cw.stack) == 1 {
		var zero T
		return zero, false
	}
	last := cw.stack[len(cw.stack)-1]
	cw.stack = cw.stack[:len(cw.stack)-1]
	return last, true
}

// PopAll removes all but the first element from the non-empty stack.
func (cw *NonEmptyStack[T]) PopAll() {
	cw.stack = cw.stack[:1]
}
