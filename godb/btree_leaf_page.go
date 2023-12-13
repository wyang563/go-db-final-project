package godb

import (
	"fmt"
)

type btreeLeafPage struct {
	b_factor int
	data    	[]*Tuple
	leftPtr	 	*BTreePage
	rightPtr 	*BTreePage
	parent	 	*BTreePage
	btreeFile	*BTreeFile
	desc 		*TupleDesc
	pageNo		int
	dirty		bool
	divideField	string
	height		int
}

// Construct a new leaf page
func newLeafPage(desc *TupleDesc, leftPtr *BTreePage, rightPtr *BTreePage, parent *BTreePage, pageNo int, divideField string, f *BTreeFile, tid TransactionID) (*btreeLeafPage, error) { // TODO: rewrite so that we take in a list of tuples rather than using the btreefile iterator
	var data []*Tuple = make([]*Tuple, 0);
	btree_it, err := f.Iterator(tid)

	if err != nil {
		return nil, fmt.Errorf("Error fetching BTreeFile iterator")
	}

	for {
		tup, err := btree_it()

		if tup == nil {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("Iterator error:" + err.Error())
		}

		data = append(data, tup)
	}

	return &btreeLeafPage{b_factor: f.b_factor, data: data, leftPtr: leftPtr, rightPtr: rightPtr, 
		                  parent: parent, btreeFile: f, desc: desc, pageNo: pageNo, divideField: divideField,
						  dirty: false}, nil;
}

// Page method - return whether or not the page is dirty
func (blp *btreeLeafPage) isDirty() bool {
	return blp.dirty 
}

// Page method - mark the page as dirty
func (blp *btreeLeafPage) setDirty(dirty bool) {
	blp.dirty = dirty;
}

// Page method - return the corresponding HeapFile
// for this page.
func (blp *btreeLeafPage) getFile() *DBFile {
	var f DBFile = blp.btreeFile;
	return &f
}

// Return a function that iterates through the tuples of the leaf page
func (blp *btreeLeafPage) tupleIter() (func() (*Tuple, error), error) {
	n := 0;
	return func() (*Tuple, error) {
		if n >= len(blp.data) {
			return nil, nil;
		}

		n++;

		return blp.data[n-1], nil;
	}, nil;
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (blp *btreeLeafPage) traverse(pageKey btreeHash) *btreeLeafPage {
	return blp;
}

