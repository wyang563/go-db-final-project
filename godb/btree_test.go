package godb

import (
	"testing"
	"strconv"
	"os"
	"fmt"
)

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
	for i := 0; i < 1000; i++ {
		name := "sam" + strconv.Itoa(i);
		tup := Tuple{Desc: td, 
					Fields: []DBValue{
						StringField{name},
						IntField{int64(i)},
					}}
		tupleList = append(tupleList, &tup);
		
	}
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

func TestOneElementBTree(t *testing.T) {
	_, tups, bf, tid, _ := makeBTreeTestVars(4);
	// create 1 element btree
	var sampleTups []*Tuple;
	sampleTups = append(sampleTups, tups[0]);
	bf.init(sampleTups);
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

func btreeIteratorHelper(b_factor int) error {
	_, tups, bf, tid, _ := makeBTreeTestVars(b_factor)
	bf.init(tups);
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

		// fmt.Println("tup ,err", tup, err)

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

	if unique_tups != len(tups) {
		// fmt.Println("unique tups:", unique_tups)
		return fmt.Errorf("Number of tuples is incorrect")
	}

	return nil
}

// tests BTree on a wide range of branching factors on inserting 30 tuples
func TestBTreeIterator(t *testing.T) {
	for b_factor := 2; b_factor < 10; b_factor++ {
		if err := btreeIteratorHelper(b_factor); err != nil {
			t.Errorf(err.Error())
		}
	}
}

// Range Scan Tests

// Tests whether traversal returns correct leaf page
func TestBTreeTraversal(t *testing.T) {
	_, tups, bf, _, _ := makeBTreeTestVars(3);
	bf.init(tups);
	// iterate over all tuples in list and see if we can find them all
	for i := 0; i < len(tups); i++ {
		searchTup := tups[i];
		resPage := bf.findLeafPage(searchTup);
		found := false;
		for _, tup := range resPage.data {
			if (tup.equals(searchTup)) {
				found = true;
			}
		}
		if !found {
			fmt.Errorf("Did not find Tuple during Traversal");
		}
	}
}

// Tests whether B+Tree implementation can scan range that takes up entire range of data values
func TestBTreeEntireRangeScan(t *testing.T) {
	_, tups, bf, tid, _ := makeBTreeTestVars(10);
	bf.init(tups);
	left := tups[0];
	right := tups[len(tups) - 1];
	rangeIter, _ := bf.SelectRange(left, right, bf.divideField, tid);
	count := 0;
	tup, _ := rangeIter();
	for tup != nil {
		// fmt.Println(count, tup);
		tup, _ = rangeIter();
		count++;
	}
	if count != len(tups) {
		fmt.Errorf("Range Scan Over All Elements Incorrect");
	}
}






