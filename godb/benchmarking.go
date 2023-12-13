package godb

import (
	// "time"
	"strconv"
	"math/rand"
)

// make random range queries, call it on btree, call it on heapfile, have to add tuples to heapfile (can do sequentially)

func setup() error {
	var desc = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}
	var tupleList []*Tuple;
	divideField := FieldExpr{selectField: desc.Fields[1]};
	b_factor := 10
	tid := NewTID()

	btree, err := NewBtreeFile("", &desc, b_factor, divideField)

	if err != nil {
		return err
	}

	heapfile := &HeapFile{file: "", desc: &desc, bufPool: NewBufferPool(1)}

	for i := 0; i < 1000; i++ {
		name := "sam" + strconv.Itoa(i);
		tup := Tuple{Desc: desc, 
					Fields: []DBValue{
						StringField{name},
						IntField{int64(i)},
					}}
		tupleList = append(tupleList, &tup);
		heapfile.insertTuple(&tup, tid)
	}

	btree.init(tupleList)

	for i := 0; i < 100; i++ {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)
		lval := r.Intn(1000)
		rval := 1000 - lval + r.Intn(lval)

		left, right := tupleList[lval], tupleList[rval]
		
		btreeIter, _ := btree.SelectRange(left, right, tid)
		heapfileIter, _ := heapfile.SelectRange(left, right, divideField, tid)
	}
	
	return nil
}

