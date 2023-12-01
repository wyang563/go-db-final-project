package godb

import (
	"errors"
	// "fmt"
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not an integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	err := os.Remove("test.dat")
	if err != nil {
		return 0, err
	}
	hf, err := NewHeapFile("test.dat", &td, NewBufferPool(10))
	if err != nil {
		return 0, err
	}
	// load csv file
	csvFile, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	err = hf.LoadFromCSV(csvFile, true, ",", false)
	if err != nil {
		return 0, err
	}
	// check if sumField exists in table
	field := FieldType{Fname: sumField, Ftype: IntType}
	fieldInd, err := findFieldInTd(field, &td)
	if err != nil {
		return 0, errors.New("Error: finding index for sumField in td")
	}
	// calculate sum of field
	tid := NewTID()
	sumRes := 0
	fileIter, _ := hf.Iterator(tid)
	res, err := fileIter()
	for res == nil {
		return 0, err
	}
	for res != nil {
		entry := res.Fields[fieldInd].(IntField)
		sumRes += int(entry.Value)
		res, _ = fileIter()
	}
	return sumRes, nil // replace me
}
