package godb

type btreeInternalPage struct {
	b_factor int
	nodes    	[]*item
	parent	 	*BTreePage
	btreeFile	*BTreeFile
	desc		*TupleDesc
	dirty		bool
	divideField	string
	height		int
}

// Construct a new internal page
func newInternalPage(desc *TupleDesc, parent *Page, divideField string, f *BTreeFile) *btreeInternalPage {
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

// Initializes root page by creating internal and leaf pages as necessary
func (bip *btreeInternalPage) init(tups []*Tuple) error {
	return nil
}

// gets the next child page based on the nodeIterNum in our list and the new node_iter_num
func (bip *btreeInternalPage) getNextIter(nodeIterNum int) (func() (*Tuple, error), error) {
	if (nodeIterNum > len(bip.nodes)) { // if node iter num is out of bounds, return nil
		return nil, nil
	}

	var child *BTreePage

	if (nodeIterNum == len(bip.nodes)) {
		child = bip.nodes[nodeIterNum - 1].rightPtr;
	} else {
		child = bip.nodes[nodeIterNum].leftPtr;
	}

	iter, err := (*child).tupleIter()

	if err != nil {
		return nil, err
	}

	return iter, nil
}

// Return a function that recursively call iterators of child nodes to then get tuples
func (bip *btreeInternalPage) tupleIter() (func() (*Tuple, error), error) { // TODO: check that this works later
	// check if items is empty first
	nodeIterNum := 0;
	// case if nodeList is empty
	if len(bip.nodes) == 0 {
		return func() (*Tuple, error) {
			return nil, nil;
		}, nil;
	}
	// iterate through left child of each item elem in nodes
	curIter, err := bip.getNextIter(nodeIterNum);
	nodeIterNum++

	if err != nil {
		return nil, err
	}

	if curIter == nil {
		return nil, nil;
	}

	return func() (*Tuple, error) {
		// if curIter isn't nil, then iterate through its tuples
		tup, _ := curIter();

		for tup == nil {
			curIter, err = bip.getNextIter(nodeIterNum);

			if err != nil {
				return nil, err
			}

			nodeIterNum++

			if curIter == nil || nodeIterNum > len(bip.nodes) {
				return nil, nil;
			}

			tup, _ = curIter();
		}

		return tup, nil;
	}, nil;
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeInternalPage) traverse(pageVal btreeHash) *btreeLeafPage {
	pVal := pageVal.pageValue;
	var i int;
	for i = 0; i < len(bip.nodes); i++ {
		divideVal := bip.nodes[i].compareVal;
		if compareDBVals(pVal, divideVal) {
			nextPage := *(bip.nodes[i].leftPtr);
			return nextPage.traverse(pageVal);
		}
	}
	// otherwise element is at right most end 
	nextPage := *(bip.nodes[i].rightPtr);
	return nextPage.traverse(pageVal);
}




