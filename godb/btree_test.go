package godb

import (
	"testing"
	"strconv"
	"os"
)

func makeBTreeTestVars() (TupleDesc, []Tuple, *BTreeFile, TransactionID) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}
	var tupleList []Tuple;
	// create list of tuples we can use in future
	for i := 0; i < 30; i++ {
		name := "sam" + strconv.Itoa(i);
		tupleList = append(tupleList, Tuple{Desc: td, 
											Fields: []DBValue{
												StringField{name},
												IntField{int64(i)},
											}});
	}
	os.Remove(TestingFile)
	var brpp = newRootPage(&td, "age", nil);
	var brptmp Page = (Page)(brpp);
	var brp *Page = &brptmp;
	// create new btree file
	bf, err := NewBtreeFile(TestingFile, &td, brp, "age");
	
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH");
		panic(err);
	}
	// set root page pointer to new file we just created
	brpp.btreeFile = bf;
	tid := NewTID();
	return td, tupleList, bf, tid;
}

func TestEmptyBTree(t *testing.T) {
	
}






