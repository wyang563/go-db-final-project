package godb

import (

)

type btreeLeafPage struct {
	b_factor int
	data    	[]*Tuple
	leftPtr	 	*Page
	rightPtr 	*Page
	parent	 	*Page
	btreeFile	*BTreeFile
	desc 		*TupleDesc
	pageNo		int
	dirty		bool
	divideField	string
}

// Construct a new leaf page
func newLeafPage(desc *TupleDesc, leftPtr *Page, rightPtr *Page, parent *Page, pageNo int, divideField string, f *BTreeFile) *btreeLeafPage {
	var data []*Tuple;
	return &btreeLeafPage{b_factor: f.b_factor, data: data, leftPtr: leftPtr, rightPtr: rightPtr, 
		                  parent: parent, btreeFile: f, desc: desc, pageNo: pageNo, divideField: divideField,
						  dirty: false};
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
func (blp *btreeLeafPage) tupleIter() func() (*Tuple, error) {
	n := 0;
	return func() (*Tuple, error) {
		if n >= len(blp.data) {
			return nil, nil;
		}
		tupVal := blp.data[n];
		n++;
		return tupVal, nil;
	}
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeLeafPage) traverse(t *Tuple) *btreeLeafPage {
	return bip;
}

