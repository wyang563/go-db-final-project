package godb

import (
	"errors"
	// "fmt"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	//add additional fields here
	distinct	 bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		return nil, errors.New("selectFields and outputNames are different lengths");
	}
	return &Project{selectFields: selectFields, outputNames: outputNames, child: child, distinct: distinct}, nil;
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	var fieldTypes []FieldType;
	for i, entry := range p.selectFields {
		ftype := entry.GetExprType();
		fieldEntry := FieldType{Fname: p.outputNames[i], TableQualifier: ftype.TableQualifier, Ftype: ftype.Ftype};
		fieldTypes = append(fieldTypes, fieldEntry);
	} 
	return &TupleDesc{Fields: fieldTypes};
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	tableIter, err := p.child.Iterator(tid);
	if err != nil {
		return nil, err;
	}
	retDesc := p.Descriptor();
	seen := make(map[any]bool);
	return func() (*Tuple, error) {
		entry, _ := tableIter();
		for entry != nil {
			_, inMap := seen[entry.tupleKey()];
			// if tuple seen already don't process it
			if !(p.distinct && inMap) {
				var retFields []DBValue;
				for _, field := range p.selectFields {
					fEntry, _ := field.EvalExpr(entry);
					retFields = append(retFields, fEntry);
				}
				seen[entry.tupleKey()] = true; 
				return &Tuple{Desc: *retDesc, Fields: retFields}, nil;
			}
			entry, _ = tableIter();
		}
		return nil, nil;
	}, nil;
}
