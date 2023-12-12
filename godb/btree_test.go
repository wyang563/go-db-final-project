package godb

import (
	"testing"
	"strconv"
	"os"
)
// TODO - change test case btrees such that they are rootpage, internalpage, leafpage (makes coding iterators easier)

func makeBTreeTestVars(b_factor int) (TupleDesc, []Tuple, *BTreeFile, TransactionID) {
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
	bf, err := NewBtreeFile(TestingFile, &td, brp, b_factor, "age");
	
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
	_, _, bf, tid := makeBTreeTestVars(2);
	// run iterator to check nothing is returned
	iter, _ := bf.Iterator(tid);
	tup, _ := iter();
	if tup != nil {
		t.Errorf("expected no tuples to be returned");
	}
}

func TestOneElementBTree(t *testing.T) {
	td, tupleList, bf, tid := makeBTreeTestVars(4);
	// create 1 element btree
	leafPage := newLeafPage(&td, nil, nil, bf.root, 0, bf.divideField, bf);
	leafPage.data = append(leafPage.data, &(tupleList[0]));
	var rootNode *btreeRootPage = (*bf.root).(*btreeRootPage);
	var leaf Page = (Page)(leafPage);
	rootNode.nodes = append(rootNode.nodes, &item{num: 2, leftPtr: &leaf, rightPtr: nil});
	count := 0;
	iter, _ := bf.Iterator(tid);
	for {
		tup, _ := iter();
		if tup == nil {
			break;
		}
		count++;
		if count > 1 {
			t.Errorf("expected there to be only 1 tuple");
		}
	}
}






