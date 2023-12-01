package godb

import (
	"fmt"
	"golang.org/x/exp/constraints"
)

type Filter[T constraints.Ordered] struct {
	op     BoolOp
	left   Expr
	right  Expr
	child  Operator
	getter func(DBValue) T
}

func intFilterGetter(v DBValue) int64 {
	intV := v.(IntField)
	return intV.Value
}

func stringFilterGetter(v DBValue) string {
	stringV := v.(StringField)
	return stringV.Value
}

// Constructor for a filter operator on ints
func NewIntFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[int64], error) {
	if constExpr.GetExprType().Ftype != IntType || field.GetExprType().Ftype != IntType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply int filter to non int-types"}
	}
	f, err := newFilter[int64](constExpr, op, field, child, intFilterGetter)
	return f, err
}

// Constructor for a filter operator on strings
func NewStringFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[string], error) {
	if constExpr.GetExprType().Ftype != StringType || field.GetExprType().Ftype != StringType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply string filter to non string-types"}
	}
	f, err := newFilter[string](constExpr, op, field, child, stringFilterGetter)
	return f, err
}

// Getter is a function that reads a value of the desired type
// from a field of a tuple
// This allows us to have a generic interface for filters that work
// with any ordered type
func newFilter[T constraints.Ordered](constExpr Expr, op BoolOp, field Expr, child Operator, getter func(DBValue) T) (*Filter[T], error) {
	return &Filter[T]{op, field, constExpr, child, getter}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter[T]) Descriptor() *TupleDesc {
	return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over
// the results of the child iterator and return a tuple if it satisfies
// the predicate.
// HINT: you can use the evalPred function defined in types.go to compare two values
func (f *Filter[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	tableIter, err := f.child.Iterator(tid);
	if err != nil {
		fmt.Println("Error when Getting Table Iterator");
		return nil, err;
	}
	return func() (*Tuple, error) {
		tup, err := tableIter();
		if err != nil {
			fmt.Println("Error Getting Tuple in Iterator");
			return nil, err;
		}
		for tup != nil {
			leftVal, err := f.left.EvalExpr(tup);
			if err != nil {
				fmt.Println("Problem Getting left val");
				return nil, err;
			}
			rightVal, err := f.right.EvalExpr(tup);
			if err != nil {
				fmt.Println("Problem Getting right val");
				return nil, err;
			}
			match := false;
			switch leftVal.(type) {
			case IntField:
				leftField := intFilterGetter(leftVal);
				rightField := intFilterGetter(rightVal);
				if evalPred(leftField, rightField, f.op) {
					match = true;
				}
			case StringField:
				leftField := stringFilterGetter(leftVal);
				rightField := stringFilterGetter(rightVal);
				if evalPred(leftField, rightField, f.op) {
					match = true;
				}
			}
			if match {
				return tup, nil;
			}
			tup, err = tableIter();
			if err != nil {
				fmt.Println("Error Getting Tuple in Iterator");
				return nil, err;
			}
		}
		return nil, nil;
	}, nil;
}