package godb

type item struct {
	num int
	leftPtr *Page
	rightPtr *Page
}

type BTreeFile struct {
	file	string
	desc	*TupleDesc
}