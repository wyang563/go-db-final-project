package godb

import (
	// "fmt"
)

type btreeRootPage struct {
	// if we want b_factor, fetch from btreeFile
	nodes       []*item
	desc        *TupleDesc
	btreeFile   *BTreeFile
	dirty       bool
	divideField FieldExpr
}

// Construct a new root page
func newRootPage(desc *TupleDesc, divideField FieldExpr, f *BTreeFile) *btreeRootPage {
	var nodes []*item;
	return &btreeRootPage{nodes: nodes, desc: desc, btreeFile: f,
		dirty: false, divideField: divideField}
}

// Page method - return whether or not the page is dirty
func (brp *btreeRootPage) isDirty() bool {
	return brp.dirty
}

// Initializes root page by creating internal and leaf pages as necessary
func (brp *btreeRootPage) init(tups []*Tuple) error {
	// fmt.Println("root:", tups)
	// make a list of tuples we split on: first element of each partition of tups starting from the second partition
	// make a list of BTreePages
	split := make([]*Tuple, 0)
	pageList := make([]*BTreePage, 0)

	psize := len(tups) / brp.btreeFile.b_factor
	var b int = brp.btreeFile.b_factor // Golang min requires upgrading version
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

		if i == b-1 {
			part = tups[i*psize:]
		} else {
			part = tups[i*psize : (i+1)*psize]
		}

		rpage := (BTreePage)(brp)

		if psize > b {
			pg = (BTreePage)(newInternalPage(brp.desc, &rpage, brp.divideField, brp.btreeFile))
			// TODO: when calling init on internal page, we need a way to find the page number
		} else {
			brp.btreeFile.LeafPages = append(
				brp.btreeFile.LeafPages, 
				newLeafPage(brp.desc, nil, nil, &rpage, brp.btreeFile.Leaves(), brp.divideField, brp.btreeFile),
			)

			pg = (BTreePage)(brp.btreeFile.LeafPages[brp.btreeFile.Leaves()-1])
			
			if brp.btreeFile.Leaves() > 1 {
				brp.btreeFile.LeafPages[brp.btreeFile.Leaves()-2].rightPtr = brp.btreeFile.LeafPages[brp.btreeFile.Leaves()-1]
				brp.btreeFile.LeafPages[brp.btreeFile.Leaves()-1].leftPtr = brp.btreeFile.LeafPages[brp.btreeFile.Leaves()-2]
			}
		}

		page = &pg
		pg.init(part)
		pageList = append(pageList, page)
	}

	// make items and link together
	for i := 0; i < b-1; i++ {
		brp.nodes = append(brp.nodes, &item{compareVal: split[i], leftPtr: pageList[i], rightPtr: pageList[i+1]})
	}

	return nil
}

// Page method - mark the page as dirty
func (brp *btreeRootPage) setDirty(dirty bool) {
	brp.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (brp *btreeRootPage) getFile() *DBFile {
	var f DBFile = brp.btreeFile
	return &f
}

// gets the next child page based on the nodeIterNum in our list and the new node_iter_num
func (brp *btreeRootPage) getNextIter(nodeIterNum int) (func() (*Tuple, error), error) {
	if nodeIterNum > len(brp.nodes) { // if node iter num is out of bounds, return nil
		return nil, nil
	}

	var child *BTreePage

	if nodeIterNum == len(brp.nodes) {
		child = brp.nodes[nodeIterNum-1].rightPtr
	} else {
		child = brp.nodes[nodeIterNum].leftPtr
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
	nodeIterNum := 0
	// case if nodeList is empty
	if len(brp.nodes) == 0 {
		return func() (*Tuple, error) {
			return nil, nil
		}, nil
	}
	// iterate through left child of each item elem in nodes
	curIter, err := brp.getNextIter(nodeIterNum)
	nodeIterNum++

	if err != nil {
		return nil, err
	}

	if curIter == nil {
		return nil, nil
	}

	return func() (*Tuple, error) {
		// if curIter isn't nil, then iterate through its tuples
		tup, _ := curIter()

		for tup == nil {
			curIter, err = brp.getNextIter(nodeIterNum)

			if err != nil {
				return nil, err
			}
			nodeIterNum++
			if curIter == nil  {
				return nil, nil
			}
			tup, _ = curIter()
		}

		return tup, nil
	}, nil
}

// Traverses tree given Tuple value and returns leaf page assosciated with tuple
func (brp *btreeRootPage) traverse(t *Tuple) *btreeLeafPage {
	var i int
	for i = 0; i < len(brp.nodes); i++ {
		divideVal := brp.nodes[i].compareVal
		compareRes, _ := t.compareField(divideVal, &brp.divideField)
		if compareRes == OrderedLessThan {
			nextPage := *(brp.nodes[i].leftPtr)
			return nextPage.traverse(t)
		}
	}
	// otherwise element is at right most end
	nextPage := *(brp.nodes[i - 1].rightPtr)
	return nextPage.traverse(t);
}
