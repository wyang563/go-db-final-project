package godb

import (
)

type btreeRootPage struct {
	// if we want b_factor, fetch from btreeFile
	nodes		[]*item
	desc		*TupleDesc
	btreeFile	*BTreeFile
	dirty		bool
	divideField string
	height 		int
}

// Construct a new root page
func newRootPage(desc *TupleDesc, divideField string, f *BTreeFile) *btreeRootPage {
	var nodes []*item;
	return &btreeRootPage{nodes: nodes, desc: desc, btreeFile: f,
		                  dirty: false, divideField: divideField};
}

// Page method - return whether or not the page is dirty
func (brp *btreeRootPage) isDirty() bool {
	return brp.dirty 
}

// Initializes root page by creating internal and leaf pages as necessary
func (brp *btreeRootPage) init(tups []*Tuple) error {
	return nil
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

// gets the next child page based on the nodeIterNum in our list and the new node_iter_num
func (brp *btreeRootPage) getNextIter(nodeIterNum int) (func() (*Tuple, error), error) {
	if (nodeIterNum > len(brp.nodes)) { // if node iter num is out of bounds, return nil
		return nil, nil
	}

	var child *BTreePage

	if (nodeIterNum == len(brp.nodes)) {
		child = brp.nodes[nodeIterNum - 1].rightPtr;
	} else {
		child = brp.nodes[nodeIterNum].leftPtr;
	}

	iter, err := (*child).tupleIter()

	if err != nil {
		return nil, err
	}

	return iter, nil
}

// Return a function that recursively call iterators of child nodes to then get tuples
func (brp *btreeRootPage) tupleIter() (func() (*Tuple, error), error) { // TODO: check that this works later
	// check if items is empty first
	nodeIterNum := 0;
	// case if nodeList is empty
	if len(brp.nodes) == 0 {
		return func() (*Tuple, error) {
			return nil, nil;
		}, nil;
	}
	// iterate through left child of each item elem in nodes
	curIter, err := brp.getNextIter(nodeIterNum);
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
			curIter, err = brp.getNextIter(nodeIterNum);

			if err != nil {
				return nil, err
			}
			nodeIterNum++
			if curIter == nil || nodeIterNum > len(brp.nodes) {
				return nil, nil;
			}
			tup, _ = curIter();
		}

		return tup, nil;
	}, nil;
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (brp *btreeRootPage) traverse(pageVal btreeHash) *btreeLeafPage {
	pVal := pageVal.pageValue;
	var i int;
	for i = 0; i < len(brp.nodes); i++ {
		divideVal := brp.nodes[i].compareVal;
		if compareDBVals(pVal, divideVal) {
			nextPage := *(brp.nodes[i].leftPtr);
			return nextPage.traverse(pageVal);
		}
	}
	// otherwise element is at right most end 
	nextPage := *(brp.nodes[i].rightPtr);
	return nextPage.traverse(pageVal);
}




