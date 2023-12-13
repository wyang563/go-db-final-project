package godb

import (
	"fmt"
)

type BTreePage interface {
	isDirty() bool
	setDirty(dirty bool)
	getFile() *DBFile
	tupleIter() (func() (*Tuple, error), error)
}

type item struct {
	num int
	leftPtr *BTreePage
	rightPtr *BTreePage
}

type btreeHash struct {
	FileName 	string
	pageValue	DBValue
}

type BTreeFile struct {
	file			string
	desc			*TupleDesc 
	root			*btreeRootPage   // root page of BTreeFile, either btree_root page or btree_leaf page
	b_factor		int
	divideField		string
	totalHeight		int
}

// Make btreeRootPage when making BtreeFile
func NewBtreeFile(fromFile string, td *TupleDesc, b_factor int, divideField string, totalHeight int) (*BTreeFile, error) { // added totalHeight parameter in case we use it later
	var file *BTreeFile = &BTreeFile{file: fromFile, desc: td, root: nil, b_factor: b_factor, divideField: divideField, totalHeight:  totalHeight}

	var brp *btreeRootPage = newRootPage(td, "age", file);
	
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

func (bf *BTreeFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return nil, fmt.Errorf("Iterator unimplemented");
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
