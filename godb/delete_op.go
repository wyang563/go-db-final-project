package godb

type DeleteOp struct {
	file	DBFile
	child	Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{file: deleteFile, child: child};
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	return i.child.Descriptor();

}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	fileIter, err := dop.child.Iterator(tid);
	if err != nil {
		return nil, err;
	}
	count := 0;
	return func() (*Tuple, error) {
		delTup, _ := fileIter();
		for delTup != nil {
			err := dop.file.deleteTuple(delTup, tid);
			if err != nil {
				return nil, err;
			}
			delTup, _ = fileIter();
			count++;
		}
		retFieldsDesc := []FieldType{FieldType{Fname: "count", TableQualifier: "", Ftype: IntType}};
		retTupleDesc := TupleDesc{Fields: retFieldsDesc};
		retFields := []DBValue{IntField{Value: int64(count)}};
		return &Tuple{Desc: retTupleDesc, Fields: retFields}, nil;
	}, nil;

}
