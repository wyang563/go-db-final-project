package godb

import (
	"errors"
	"sort"
	"fmt"
)

type btreeLeafPage struct {
	b_factor    int
	data        []*Tuple
	leftPtr     *BTreePage
	rightPtr    *BTreePage
	parent      *BTreePage
	btreeFile   *BTreeFile
	desc        *TupleDesc
	pageNo      int
	dirty       bool
	divideField FieldExpr
}

// Construct a new leaf page
func newLeafPage(desc *TupleDesc, leftPtr *BTreePage, rightPtr *BTreePage, parent *BTreePage, pageNo int, divideField FieldExpr, f *BTreeFile) *btreeLeafPage { // TODO: rewrite so that we take in a list of tuples rather than using the btreefile iterator
	var data []*Tuple = make([]*Tuple, 0)

	return &btreeLeafPage{b_factor: f.b_factor, data: data, leftPtr: leftPtr, rightPtr: rightPtr,
		parent: parent, btreeFile: f, desc: desc, pageNo: pageNo, divideField: divideField,
		dirty: false}
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
	var f DBFile = blp.btreeFile
	return &f
}

// Initializes root page by creating internal and leaf pages as necessary
func (blp *btreeLeafPage) init(tups []*Tuple) error {
	fmt.Println("leaf:", tups)
	blp.data = tups
	return nil
}

// Return a function that iterates through the tuples of the leaf page
func (blp *btreeLeafPage) tupleIter() (func() (*Tuple, error), error) {
	n := 0
	return func() (*Tuple, error) {
		if n >= len(blp.data) {
			return nil, nil
		}
		n++
		return blp.data[n-1], nil
	}, nil
}

func (blp *btreeLeafPage) insertTuple(t *Tuple) error {
	if len(blp.data) >= blp.b_factor {
		return errors.New("data array full")
	}
	blp.data = append(blp.data, t)
	// sort array according to the divide field
	sort.Slice(blp.data, func(i int, j int) bool {
		res, _ := blp.data[i].compareField(blp.data[j], &blp.divideField);
		return res == OrderedLessThan;
	});
	return nil
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (blp *btreeLeafPage) traverse(t *Tuple) *btreeLeafPage {
	return blp;
}
