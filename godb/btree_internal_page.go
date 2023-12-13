package godb

import (
	"fmt"
)

type btreeInternalPage struct {
	b_factor int
	nodes    	[]*item
	parent	 	*BTreePage
	btreeFile	*BTreeFile
	desc		*TupleDesc
	dirty		bool
	divideField	FieldExpr
}

// Construct a new internal page
func newInternalPage(desc *TupleDesc, parent *BTreePage, divideField FieldExpr, f *BTreeFile) *btreeInternalPage {
	var nodes []*item;
	return &btreeInternalPage{b_factor: f.b_factor, nodes: nodes, parent: parent, btreeFile: f, 
		                      desc: desc, dirty: false, divideField: divideField};
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
	fmt.Println("internal:", tups)
	// make a list of tuples we split on: first element of each partition of tups starting from the second partition
	// make a list of BTreePages
	split := make([]*Tuple, 0)
	pageList := make([]*BTreePage, 0)

	psize := len(tups)/bip.btreeFile.b_factor
	var b int = bip.btreeFile.b_factor // Golang min requires upgrading version
	if len(tups) < b {
		b = len(tups)
	}

	for i := 0; i < b; i++ {
		if i > 0 {
			split = append(split, tups[i*psize])
		}

		var part []*Tuple
		var pg BTreePage
		var page *BTreePage

		if i == b - 1 {
			part = tups[i*psize:]
		} else {
			part = tups[i*psize:(i+1)*psize]
		}

		rpage := (BTreePage)(bip)

		if psize > b {
			pg = (BTreePage)(newInternalPage(bip.desc, &rpage, bip.divideField, bip.btreeFile))
			// TODO: when calling init on internal page, we need a way to find the page number
		} else {
			pg = (BTreePage)(newLeafPage(bip.desc, nil, nil, &rpage, bip.btreeFile.Leaves(), bip.divideField, bip.btreeFile))
			bip.btreeFile.LeafInc()
		}

		page = &pg
		(*page).init(part)
		pageList = append(pageList, page)
	}

	// make items and link together
	for i := 0; i < b - 1; i++ {
		fmt.Println("bip:", split, i, pageList)
		bip.nodes = append(bip.nodes, &item{compareVal: split[i], leftPtr: pageList[i], rightPtr: pageList[i+1]})
	}

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
func (bip *btreeInternalPage) traverse(t *Tuple) *btreeLeafPage {
	var i int
	for i = 0; i < len(bip.nodes); i++ {
		divideVal := bip.nodes[i].compareVal
		compareRes, _ := t.compareField(divideVal, &bip.divideField)
		if compareRes == OrderedLessThan {
			nextPage := *(bip.nodes[i].leftPtr)
			return nextPage.traverse(t)
		}
	}
	// otherwise element is at right most end
	nextPage := *(bip.nodes[i].rightPtr)
	return nextPage.traverse(t);
}




