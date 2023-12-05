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

}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeInternalPage) traverse(t *Tuple) *btreeLeafPage {

}

// Merges 



