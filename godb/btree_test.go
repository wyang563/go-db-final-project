package godb

import (
	"testing"
	"os"
)


func makeBTreeTestVars() (TupleDesc, Tuple, Tuple, *BTreeFile, *BufferPool, TransactionID) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{999},
		}}

	os.Remove(TestingFile)
	var brp *Page = (newRootPage(&td, "age", nil)).(*Page);
	brp = brp.(Page);
	bf, err := NewBtreeFile(TestingFile, &td, brp, "age");
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return td, t1, t2, hf, bp, tid

}


