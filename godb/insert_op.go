package godb

// TODO: some code goes here
type InsertOp struct {
	file	DBFile
	child	Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{file: insertFile, child: child};
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return i.child.Descriptor();
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	fileIter, err := iop.child.Iterator(tid);
	if err != nil {
		return nil, err;
	}
	count := 0;
	return func() (*Tuple, error) {
		insertTup, _ := fileIter();
		for insertTup != nil {
			err := iop.file.insertTuple(insertTup, tid);
			if err != nil {
				return nil, err;
			}
			insertTup, _ = fileIter();
			count++;
		}
		retFieldsDesc := []FieldType{FieldType{Fname: "count", TableQualifier: "", Ftype: IntType}};
		retTupleDesc := TupleDesc{Fields: retFieldsDesc};
		retFields := []DBValue{IntField{Value: int64(count)}};
		return &Tuple{Desc: retTupleDesc, Fields: retFields}, nil;
	}, nil;
}
