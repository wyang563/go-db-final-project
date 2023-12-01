package godb

import (
	"sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy 	[]Expr // OrderBy should include these two fields (used by parser)
	child   	Operator
	ascending	[]bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil;
}

func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor();
} 

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	tableIter, err := o.child.Iterator(tid);
	var tupleList []*Tuple;
	if err != nil {
		return nil, err;
	}
	// get all entries from tableIter
	entry, _ := tableIter();
	for entry != nil {
		tupleList = append(tupleList, entry);
		entry, _ = tableIter();
	}
	// sort values
	sort.Slice(tupleList, func(i int, j int) bool {
		for k, attrOrder := range o.ascending {
			val1, _ := o.orderBy[k].EvalExpr(tupleList[i]);
			val2, _ := o.orderBy[k].EvalExpr(tupleList[j]);
			switch val1.(type) {
			case IntField:
				v1 := val1.(IntField).Value;
				v2 := val2.(IntField).Value;
				if v1 != v2 {
					if attrOrder {
						return v1 < v2;
					}
					return v1 > v2;
				}
			case StringField:
				v1 := val1.(StringField).Value;
				v2 := val2.(StringField).Value;
				if v1 != v2 {
					if attrOrder {
						return v1 < v2;
					}
					return v1 > v2;
				}
			}
		}
		return true;
	});
	index := 0;
	return func() (*Tuple, error) {
		if index < len(tupleList) {
			index++;
			return tupleList[index-1], nil;
		}
		return nil, nil;
	}, nil;
}
