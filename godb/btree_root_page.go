package godb

type btreeRootPage struct {
	b_factor	int
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
	return &btreeRootPage{b_factor: f.b_factor, nodes: nodes, desc: desc, btreeFile: f,
		                  dirty: false, divideField: divideField};
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

// gets the next child page based on the nodeIterNum in our list and the new node_iter_num
func (brp *btreeRootPage) getNextIter(nodeIterNum int) (func() (*Tuple, error), int) {
	var child *btreeInternalPage = nil;
	for child == nil {
		nodeIterNum++;
		if (nodeIterNum > len(brp.nodes)) {
			return nil, nodeIterNum;
		}
		// check if next node is internal node or leaf node based off height
		// otherwise it's an internal leaf node
		if (brp.height == brp.btreeFile.totalHeight - 2) {
			// check if we're on the final right node
			var child *btreeLeafPage;
			if (nodeIterNum == len(brp.nodes)) {
				child = (*(brp.nodes[nodeIterNum - 1].rightPtr)).(*btreeLeafPage);
			} else {
				child = (*(brp.nodes[nodeIterNum].leftPtr)).(*btreeLeafPage);
			}
			// check if child is valid
			if child != nil {
				curIter, _ := child.tupleIter();
				return curIter, nodeIterNum;
			}
		} else {
			var child *btreeInternalPage;
			if (nodeIterNum == len(brp.nodes)) {
				child = (*(brp.nodes[nodeIterNum - 1].rightPtr)).(*btreeInternalPage);
			} else {
				child = (*(brp.nodes[nodeIterNum].leftPtr)).(*btreeInternalPage);
			}
			// check if child is empty/valid
			if child != nil {
				curIter, _ := child.tupleIter();
				return curIter, nodeIterNum;
			}
		}	
	}
	return nil, nodeIterNum;
}

// Return a function that recursively call iterators of child nodes to then get tuples
func (brp *btreeRootPage) tupleIter() (func() (*Tuple, error), error) {
	// check if items is empty first
	nodeIterNum := -1;
	// case if nodeList is empty
	if len(brp.nodes) == 0 {
		return func() (*Tuple, error) {
			return nil, nil;
		}, nil;
	}
	// iterate through left child of each item elem in nodes
	curIter, nodeIterNum := brp.getNextIter(nodeIterNum);
	if curIter == nil {
		return nil, nil;
	}
	return func() (*Tuple, error) {
		// if curIter isn't nil, then iterate through its tuples
		tup, _ := curIter();
		for tup == nil {
			curIter, nodeIterNum = brp.getNextIter(nodeIterNum);
			if curIter == nil || nodeIterNum > len(brp.nodes) {
				return nil, nil;
			}
			tup, _ = curIter();
		}
		return tup, nil;
	}, nil;
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (bip *btreeRootPage) traverse(pageVal btreeHash) *btreeLeafPage {
	return nil;
}




