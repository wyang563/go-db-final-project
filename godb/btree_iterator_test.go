package godb

import (
	"testing"
	"fmt"
)

func testBTreeIterator(b_factor int) error {
	_, tups, bf, tid := makeBTreeTestVars(b_factor)
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
		return fmt.Errorf("Number of tuples is incorrect")
	}

	return nil
}

func TestBTreeIterator(t *testing.T) {
	for b_factor := 1; b_factor < 10; b_factor++ {
		if err := testBTreeIterator(b_factor); err != nil {
			t.Errorf(err.Error())
		}
	}
}
