package godb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// make random range queries, call it on btree, call it on heapfile, have to add tuples to heapfile (can do sequentially)

func TestBTreeTime(t *testing.T) {
	var desc = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}
	var tupleList []*Tuple
	divideField := FieldExpr{selectField: desc.Fields[1]}
	b_factor := 10
	tid := NewTID()
	bp := NewBufferPool(3)
	os.Remove(TestingFile)
	heapfile, _ := NewHeapFile(TestingFile, &desc, bp)
	btree, _ := NewBtreeFile("", &desc, b_factor, divideField)

	// if err != nil {
	// 	return err
	// }

	for i := 0; i < 10000; i++ {
		name := "sam" + strconv.Itoa(i)
		tup := Tuple{Desc: desc,
			Fields: []DBValue{
				StringField{name},
				IntField{int64(i)},
			}}
		tupleList = append(tupleList, &tup)
		heapfile.insertTuple(&tup, tid)
	}

	btree.init(tupleList)
	btreeTime := 0
	heapTime := 0
	iters := 1000
	for i := 0; i < iters; i++ {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)
		lval := r.Intn(9000)
		width := r.Intn(100)
		rval := lval + width
		left, right := tupleList[lval], tupleList[rval]

		btreeIter, _ := btree.SelectRange(left, right, tid)
		heapfileIter, _ := heapfile.Iterator(tid)
		// time b+tree range query
		start := time.Now()
		tup, _ := btreeIter()
		for tup != nil {
			tup, _ = btreeIter()
		}
		elapsed := time.Since(start)
		btreeTime += int(elapsed)
		// time heapfile range query
		start = time.Now()
		tup, _ = heapfileIter()
		count := 0
		for tup != nil {
			tup, _ = heapfileIter()
			count++
		}
		elapsed = time.Since(start)
		heapTime += int(elapsed)
	}
	fmt.Println(heapfile.bufPool)
	fmt.Println("Average heap Time is - ", heapTime/iters)
	fmt.Println("Average b+tree Time is - ", btreeTime/iters)
	// return nil;
}
