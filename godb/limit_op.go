package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim};
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor();

}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0;
	tableIter, err := l.child.Iterator(tid);
	if err != nil {
		return nil, err;
	}
	limitVal, _ := l.limitTups.EvalExpr(nil);
	limit := int((limitVal.(IntField)).Value);
	return func() (*Tuple, error) {
		entry, _ := tableIter();
		if count >= limit || entry == nil {
			return nil, nil;
		}
		count++;
		return entry, nil;
	}, nil;
}
