package godb

type btreeRootPage struct {
	b_factor	int
	nodes		[]*item
	desc		*TupleDesc
	dirty		bool
	divideField	string
}

