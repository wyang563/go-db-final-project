package godb

import (
	"fmt"
	"sort"
)

type item struct {
	compareVal *Tuple
	leftPtr *BTreePage
	rightPtr *BTreePage
}

type btreeHash struct {
	FileName 	string
	pageValue	int
}

type BTreePage interface {
	isDirty() bool
	setDirty(dirty bool)
	getFile() *DBFile
	tupleIter() (func() (*Tuple, error), error)
	traverse(t *Tuple) *btreeLeafPage
	init(tups []*Tuple) error
}

type BTreeFile struct {
	file			string
	desc			*TupleDesc 
	root			*btreeRootPage   // root page of BTreeFile, either btree_root page or btree_leaf page
	b_factor		int
	divideField		FieldExpr // field expr we can use on tuples to extract element we're dividing on
	LeafPages		[]*btreeLeafPage
}

// Make btreeRootPage when making BtreeFile
func NewBtreeFile(fromFile string, td *TupleDesc, b_factor int, divideField FieldExpr) (*BTreeFile, error) { // added totalHeight parameter in case we use it later
	var leafPages []*btreeLeafPage;
	var file *BTreeFile = &BTreeFile{file: fromFile, desc: td, root: nil, b_factor: b_factor, divideField: divideField, LeafPages: leafPages};

	var brp *btreeRootPage = newRootPage(td, divideField, file);
	
	file.setRootPage(brp) // assigns btreeRootPage
	fmt.Print("")
	// read tuples in fromFile, insert into root page, root page creates child pages as needed
	return file, nil;
}

func (bf *BTreeFile) pageKey(PageNo int) any {
	return btreeHash{FileName: bf.file, pageValue: PageNo};
}

func (bf *BTreeFile) setRootPage(page *btreeRootPage) {
	bf.root = page
}


func (bf *BTreeFile) Leaves() int {
	return len(bf.LeafPages)
}

// reads page given a specific page number
func (bf *BTreeFile) readPage(pageNo int) (*Page, error) {
	tempPage := bf.LeafPages[pageNo];
	var resPage Page = (Page)(tempPage);
	return &resPage, nil;
}

func (bf *BTreeFile) findLeafPage(t *Tuple) (*btreeLeafPage) {
	resPage := bf.root.traverse(t);
	return resPage;
}

func (bf *BTreeFile) Descriptor() *TupleDesc {
	return bf.desc;
}

func (bf *BTreeFile) SelectRange(left, right *Tuple, compareField FieldExpr, tid TransactionID) (func() (*Tuple, error), error) {
	// traverse to find left bound page
	leafPage := bf.findLeafPage(left);
	curIter, _ := leafPage.tupleIter();
	startCount := 0;
	for i := 0; i < len(leafPage.data) - 1; i++ {
		compareRes0, _ := left.compareField(leafPage.data[i], &bf.divideField);
		compareRes1, _ := left.compareField(leafPage.data[i + 1], &bf.divideField)
		if compareRes0 == OrderedLessThan || compareRes0 == OrderedEqual {
			startCount = i;
			break;
		} else if compareRes1 == OrderedLessThan || compareRes1 == OrderedEqual {
			startCount = i + 1;
			break;
		}
	}
	// iterate up to start count tuple
	for i := 0; i < startCount; i++ {
		curIter();
	}
	return func() (*Tuple, error) {
		tup, _ := curIter();
		for tup == nil {
			leafPage := leafPage.rightPtr;
			if (leafPage == nil) {
				return nil, nil;
			}
			curIter, _ = leafPage.tupleIter();
			tup, _ = curIter();
		}
		compareRes, _ := tup.compareField(right, &bf.divideField);
		// check if current value is out of range
		if compareRes == OrderedGreaterThan {
			return nil, nil;
		}
		// fmt.Println(tup);	
		return tup, nil;
	}, nil;
}

func (bf *BTreeFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, _ := bf.root.tupleIter();
	return iter, nil;
}

// Initialize tuples in BTreeFile
func (bf *BTreeFile) init(tups []*Tuple) error {
	// sort tuples here before initing the full B+Tree
	sort.Slice(tups, func(i int, j int) bool {
		res, _ := tups[i].compareField(tups[j], &bf.divideField);
		return res == OrderedLessThan;
	});
	return bf.root.init(tups)
}

// dummy functions just so we implement DBFile
func (bf *BTreeFile) flushPage(page *Page) error {
	return nil;
}

func (bf *BTreeFile) insertTuple(t *Tuple, tid TransactionID) error {
	return nil;
}

func (bf *BTreeFile) deleteTuple(t *Tuple, tid TransactionID) error {
	return nil;
}
