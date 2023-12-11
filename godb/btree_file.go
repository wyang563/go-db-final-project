package godb

type item struct {
	num int
	leftPtr *Page
	rightPtr *Page
}

type btreeHash struct {
	FileName 	string
	pageValue	DBValue
}

type BTreeFile struct {
	file		string
	desc		*TupleDesc 
	root		*Page   // root page of BTreeFile, either btree_root page or btree_leaf page
	height		int		// height of B+Tree 
	divideField	string
}

func (bf *BTreeFile) pageKey(pageValue int) any {
	return btreeHash{FileName: bf.file, pageValue: pageValue};
}

// reads page given a specific pageKey hash
func (bf *BTreeFile) readPage(pageNo int) (*Page, error) {

}

func (bf *BTreeFile) readPageByKey(pageVal btreeHash) (*Page, error) {
	
}

func (bf *BTreeFile) Descriptor() *TupleDesc {
	return bf.desc;
}

func (bf *BTreeFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {

}
