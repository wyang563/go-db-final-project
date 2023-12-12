package godb

type btreeInternalPage struct {
	b_factor int
	nodes    	[]*item
	parent	 	*Page
	btreeFile	*BTreeFile
	desc		*TupleDesc
	dirty		bool
	divideField	string
}

// Construct a new internal page
func newInternalPage(desc *Tuple, parent *Page, divideField string, f *BTreeFile) *btreeInternalPage {
	return nil;
}

// Page method - return whether or not the page is dirty
func (bip *btreeInternalPage) isDirty() bool {
	return bip.dirty 
}

// Page method - mark the page as dirty
func (bip *btreeInternalPage) setDirty(dirty bool) {
	bip.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (bip *btreeInternalPage) getFile() *DBFile {
	var f DBFile = bip.btreeFile;
	return &f
}

// Return a function that recursively call iterators of child nodes to then get tuples
func (bip *btreeInternalPage) tupleIter() func() (*Tuple, error) {
	return nil;
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeInternalPage) traverse(t *Tuple) *btreeLeafPage {
	return nil;
}




