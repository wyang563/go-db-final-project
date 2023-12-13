package godb

import (
	"testing"
	"strconv"
	"os"
	"fmt"
)
// TODO - change test case btrees such that they are rootpage, internalpage, leafpage (makes coding iterators easier)

func makeBTreeTestVars(b_factor int) (TupleDesc, []*Tuple, *BTreeFile, TransactionID, FieldExpr) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}
	divideField := FieldExpr{selectField: td.Fields[1]};
	var tupleList []*Tuple;

	// create new btree file
	bf, err := NewBtreeFile(TestingFile, &td, b_factor, divideField);
	tid := NewTID();
	// create list of tuples we can use in future
	for i := 0; i < 30; i++ {
		name := "sam" + strconv.Itoa(i);
		tup := Tuple{Desc: td, 
					Fields: []DBValue{
						StringField{name},
						IntField{int64(i)},
					}}
		tupleList = append(tupleList, &tup);
		
	}
	bf.init(tupleList)
	os.Remove(TestingFile)
	
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH");
		panic(err);
	}
	return td, tupleList, bf, tid, divideField;
}

func TestEmptyBTree(t *testing.T) {
	_, _, bf, tid, _ := makeBTreeTestVars(2);
	// run iterator to check nothing is returned
	iter, _ := bf.Iterator(tid);
	tup, _ := iter();
	if tup != nil {
		t.Errorf("expected no tuples to be returned");
	}
}

// func TestOneElementBTree(t *testing.T) {
// 	td, tupleList, bf, tid := makeBTreeTestVars(4);
// 	// create 1 element btree
// 	leafPage, err := newLeafPage(&td, nil, nil, bf.root, 0, bf.divideField, bf, tid);

// 	if err != nil {
// 		t.Errorf("Error creating leaf page:" + err.Error())
// 	}

// 	leafPage.data = append(leafPage.data, &(tupleList[0]));
// 	var rootNode *btreeRootPage = (*bf.root).(*btreeRootPage);
// 	var leaf Page = (Page)(leafPage);
// 	rootNode.nodes = append(rootNode.nodes, &item{num: 2, leftPtr: &leaf, rightPtr: nil});
// 	count := 0;
// 	iter, _ := bf.Iterator(tid);
// 	for {
// 		tup, _ := iter();
// 		if tup == nil {
// 			break;
// 		}
// 		count++;
// 		if count > 1 {
// 			t.Errorf("expected there to be only 1 tuple");
// 		}
// 	}
// }

func testBTreeIterator(b_factor int) error {
	_, tups, bf, tid, _ := makeBTreeTestVars(b_factor)
	tupset := make(map[int64] int)

	for _, tup := range tups {
		tupset[tup.Fields[1].(IntField).Value] = 1
	}

	btree_it, err := bf.Iterator(tid)

	if err != nil {
		return err
	}

	unique_tups := 0 // there should be 30 unique tuples
	var curr_age int64 = -1

	for {
		tup, err := btree_it()

		fmt.Println("tup ,err", tup, err)

		if tup == nil {
			break
		}

		if err != nil {
			return fmt.Errorf("Iterator error:" +  err.Error())
		}

		if tupset[(*tup).Fields[1].(IntField).Value] != 1 {
			fmt.Errorf("Iterated over tuple that does not exist")
		}

		unique_tups++
		tupset[(*tup).Fields[1].(IntField).Value] = 0

		if (*tup).Fields[1].(IntField).Value != curr_age + 1 { // checks age
			return fmt.Errorf("Must iterate in sorted order")
		}
		
		curr_age++
	}

	if unique_tups != 30 {
		fmt.Println("unique tups:", unique_tups)
		return fmt.Errorf("Number of tuples is incorrect")
	}

	return nil
}

func TestBTreeIterator(t *testing.T) {
	for b_factor := 2; b_factor < 10; b_factor++ {
		if err := testBTreeIterator(b_factor); err != nil {
			t.Errorf(err.Error())
		}
	}
}






