package godb

type btreeRootPage struct {
	b_factor	int
	nodes		[]*item
	desc		*TupleDesc
	btreeFile	*BTreeFile
	dirty		bool
	divideField string
}

// Construct a new root page
func newRootPage(desc *TupleDesc, divideField string, f *BTreeFile) *btreeRootPage {

}

// Page method - return whether or not the page is dirty
func (brp *btreeRootPage) isDirty() bool {
	return brp.dirty 
}

// Page method - mark the page as dirty
func (brp *btreeRootPage) setDirty(dirty bool) {
	brp.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (brp *btreeRootPage) getFile() *DBFile {
	var f DBFile = brp.btreeFile;
	return &f
}

// Return a function that recursively call iterators of child nodes to then get tuples
func (brp *btreeRootPage) tupleIter() func() (*Tuple, error) {

}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeRootPage) traverse(t *Tuple) *btreeLeafPage {

}




