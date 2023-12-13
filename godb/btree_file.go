package godb

import (
	// "fmt"
)

type item struct {
	compareVal *Tuple
	leftPtr *BTreePage
	rightPtr *BTreePage
}

type btreeHash struct {
	FileName 	string
	pageValue	int
}

type BTreePage interface {
	isDirty() bool
	setDirty(dirty bool)
	getFile() *DBFile
	tupleIter() (func() (*Tuple, error), error)
	traverse(pageVal btreeHash) *btreeLeafPage
	init(tups []*Tuple) error
}

type BTreeFile struct {
	file			string
	desc			*TupleDesc 
	root			*btreeRootPage   // root page of BTreeFile, either btree_root page or btree_leaf page
	b_factor		int
	divideField		FieldExpr
	totalHeight		int
	leafNum			int
}

// Make btreeRootPage when making BtreeFile
func NewBtreeFile(fromFile string, td *TupleDesc, b_factor int, divideField FieldExpr, totalHeight int) (*BTreeFile, error) { // added totalHeight parameter in case we use it later
	var file *BTreeFile = &BTreeFile{file: fromFile, desc: td, root: nil, b_factor: b_factor, divideField: divideField, totalHeight:  totalHeight}

	var brp *btreeRootPage = newRootPage(td, divideField, file);
	
	file.setRootPage(brp) // assigns btreeRootPage

	// read tuples in fromFile, insert into root page, root page creates child pages as needed
	return file, nil;
}

func (bf *BTreeFile) pageKey(pageValue int) any {
	return btreeHash{FileName: bf.file, pageValue: pageValue};
}

func (bf *BTreeFile) setRootPage(page *btreeRootPage) {
	bf.root = page
}

func (bf *BTreeFile) LeafInc() {
	bf.leafNum++
}

func (bf *BTreeFile) Leaves() int {
	return bf.leafNum
}

// reads page given a specific pageKey hash
func (bf *BTreeFile) readPage(pageNo int) (*Page, error) {
	return nil, nil;
}

func (bf *BTreeFile) readPageByKey(pageVal btreeHash) (*Page, error) {
	return nil, nil;
}

func (bf *BTreeFile) Descriptor() *TupleDesc {
	return bf.desc;
}

func (bf *BTreeFile) SelectRange(left, right *Tuple, compareField FieldExpr, tid TransactionID) (func() (*Tuple, error), error) {
	// traverse to find left bound page
	val, _ := compareField.EvalExpr(left);
	temp := bf.pageKey(val);
	var leftHash btreeHash = temp.(btreeHash);
	leafPage := bf.root.traverse(leftHash);
	curIter, _ := leafPage.tupleIter();
	startCount := 0;
	for i := 0; i < len(leafPage.data) - 1; i++ {
		tupVal0, _ := compareField.EvalExpr(leafPage.data[i]);
		tupVal1, _ := compareField.EvalExpr(leafPage.data[i + 1])
		if compareDBVals(val, tupVal0) {
			startCount = i;
			break;
		} else if compareDBVals(val, tupVal1) {
			startCount = i + 1;
			break;
		}
	}
	// iterate up to start count tuple
	for i := 0; i < startCount - 1; i++ {
		curIter();
	}
	// get starting tuple
	tup, _ := curIter();
	return func() (*Tuple, error) {
		for tup == nil {
			leafPage = (*(leafPage.rightPtr)).(btreeLeafPage);
		}	
		return nil, nil;
	}, nil;
}

func (bf *BTreeFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, _ := bf.root.tupleIter();
	return iter, nil;
}

// Initialize tuples in BTreeFile
func (bf *BTreeFile) init(tups []*Tuple) error {
	// TODO: sort tuples here
	return bf.root.init(tups)
}

// dummy functions just so we implement DBFile
func (bf *BTreeFile) flushPage(page *Page) error {
	return nil;
}

func (bf *BTreeFile) insertTuple(t *Tuple, tid TransactionID) error {
	return nil;
}

func (bf *BTreeFile) deleteTuple(t *Tuple, tid TransactionID) error {
	return nil;
}
