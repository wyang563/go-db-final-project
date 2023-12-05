package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
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
func newLeafPage(desc *Tuple, leftPtr *Page, rightPtr *Page, parent *Page, pageNo int, divideField string, f *BTreeFile) *btreeLeafPage {

}

// Insert the tuple into a free slot on the leaf page, or return an error 
// if there are no more free slots. Set tuple RID and return it.

func (blp *btreeLeafPage) insertTuple(t *Tuple) (recordID, error) {
	return t.Rid, nil;
}

// Delete the tuple in the specified slot number, or return an error if that
// slot number is invalid
func (blp *btreeLeafPage) deleteTuple(rid recordID) error {
	return nil;
}

// Page method - return whether or not the page is dirty
func (blp *btreeLeafPage) isDirty() bool {
	return blp.dirty 
}

// Page method - mark the page as dirty
func (blp *btreeLeafPage) setDirty(dirty bool) {
	blp.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (blp *btreeLeafPage) getFile() *DBFile {
	var f DBFile = blp.btreeFile;
	return &f
}


// Allocates a new bytes.Buffer and write the btree leaf page to it. Returns an
// error if this write operation fails. 
func (blp *btreeLeafPage) toBuffer() (*bytes.Buffer, error) {

}

// Read the contents of the Btree Leaf Page from the supplied buffer
func (blp *btreeLeafPage) initFromBuffer(buf *bytes.Buffer) error {
	return nil;	
}

// Return a function that iterates through the tuples of the leaf page
func (blp *btreeLeafPage) tupleIter() func() (*Tuple, error) {

}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeLeafPage) traverse(t *Tuple) *btreeLeafPage {
	return bip;
}

