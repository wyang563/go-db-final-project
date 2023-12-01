package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	// "errors"
)

// HeapFile is an unordered collection of tuples. Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	file			string
	desc			*TupleDesc
	bufPool 		*BufferPool
	m				sync.Mutex
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// init test.dat if not there
	if f, err := os.OpenFile(fromFile, os.O_CREATE|os.O_RDONLY, 0644); err != nil {
		return nil, err
	} else {
		f.Close()
	}
	return &HeapFile{file: fromFile, desc: td, bufPool: bp}, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	fileInfo, err := os.Stat(f.file)
	if err != nil {
		fmt.Println("unable to read file info")
		return 0
	}
	fileSize := int(fileInfo.Size())
	return fileSize / PageSize
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}
		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// calculate offset
	offset := int64(pageNo * PageSize)
	// read from buffer pool if pageNo exists in it
	readData := make([]byte, PageSize)
	fi, err := os.Open(f.file)
	// fileSize, _ := fi.Stat();
	// fmt.Println(fileSize.Size());
	if err != nil {
		fmt.Println("error reading file")
		return nil, err
	}
	// fmt.Println(fi);
	_, err = fi.ReadAt(readData, offset)
	if err != nil {
		// fmt.Println("error reading file at offset:", offset, "error:", err, "file:", f.file);
		return nil, err
	}
	fi.Close()
	// store data to new page
	p := newHeapPage(f.desc, pageNo, f)
	err = p.initFromBuffer(bytes.NewBuffer(readData))
	if err != nil {
		fmt.Println("Error Calling InitFromBuffer");
		return nil, err;
	}
	var res Page = p
	return &res, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot it finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// iterate through pages in BufPool to check if there's space
	f.m.Lock();
	bufPoolMap := f.bufPool.PageNoToPage;
	f.m.Unlock();
	for key, page := range bufPoolMap {
		f.m.Lock();
		slotFile := key.FileName;
		f.m.Unlock();
		if slotFile != f.file {
			continue;
		}
		var p *heapPage = (*page).(*heapPage)
		// if page is not full insert tuple
		if p.remSlots > 0 {
			f.m.Lock();
			_, err := p.insertTuple(t)
			f.m.Unlock();
			if err != nil {
				fmt.Println("error inserting into page on buf pool")
				return err
			}
			(*page).setDirty(true);
			return nil
		}
	}
	// check if there is space in a page in heap file
	f.m.Lock();
	totalPages := f.NumPages();
	f.m.Unlock();
	for i := 0; i < totalPages; i++ {
		page, err := f.bufPool.GetPage(f, i, tid, ReadPerm);
		if err != nil {
			fmt.Println("error reading page in insertTuple")
			return err
		}
		var p *heapPage = (*page).(*heapPage)
		if p.remSlots > 0 {
			f.m.Lock();
			_, err := p.insertTuple(t)
			f.m.Unlock();
			if err != nil {
				fmt.Println("error inserting into page on file")
				return err
			}
			//pagePointer := (Page)(p)
			(*page).setDirty(true)
			return nil
		}
	}
	// create new heap page if no available entries found
	f.m.Lock();
	newPageNo := f.NumPages()
	newPage := newHeapPage(f.desc, newPageNo, f)
	newPagePointer := (Page)(newPage)
	f.flushPage(&newPagePointer)
	f.m.Unlock();
	page, err := f.bufPool.GetPage(f, newPageNo, tid, WritePerm);
	if err != nil {
		fmt.Println("error reading page in insertTuple")
		return err
	}
	var heapPage = (*page).(*heapPage);
	// flush page to buffer
	_, err = heapPage.insertTuple(t)
	if err != nil {
		fmt.Println("error inserting tuple")
		return err
	}
	heapPage.setDirty(true);
	return nil //replace me
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// get info from RID
	rid := t.Rid.(RID)
	pageNo := rid.pageNo;
	page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	if err != nil {
		// fmt.Println("Get Page func failed in deleteTuple")
		return err
	}
	var p *heapPage = (*page).(*heapPage)
	// fmt.Println(rid);
	// fmt.Println(p.slots);
	err = p.deleteTuple(rid)
	if err != nil {
		fmt.Println("Delete Tuple failed in deleteTuple")
		fmt.Println(err);
		return err
	}
	p.setDirty(true);
	// pagePointer := (Page)(p)
	if err != nil {
		fmt.Println("Flush Page failed in deleteTuple")
		return err
	}
	return nil //replace me
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	var page *heapPage = (*p).(*heapPage)
	offset := page.pageNo * PageSize
	dataBuffer, err := page.toBuffer()
	if err != nil {
		fmt.Println("error converting given page to buffer")
		return err
	}
	// write buffer data to file
	fi, err := os.OpenFile(f.file, os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("error opening file")
		return err
	}
	_, err = fi.WriteAt(dataBuffer.Bytes(), int64(offset))
	if err != nil {
		fmt.Println("error writing buffer to file")
		return err
	}
	fi.Close()
	(*p).setDirty(false);
	return nil //replace me
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.desc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iterPageNo := 0
	if f.NumPages() == 0 {
		return func() (*Tuple, error) {
			return nil, nil;
		}, nil;
	}
	rawPage, err := f.bufPool.GetPage(f, 0, tid, ReadPerm)
	if err != nil {
		return nil, err
	}
	var curPage *heapPage = (*rawPage).(*heapPage)
	curPageIter := curPage.tupleIter()
	return func() (*Tuple, error) {
		// check if we still have pages left
		f.m.Lock();
		numPages := f.NumPages();
		f.m.Unlock();
		if iterPageNo >= numPages {
			return nil, nil
		}
		tup, _ := curPageIter()
		for tup == nil {
			iterPageNo++
			if iterPageNo >= numPages {
				return nil, nil
			}
			rawPage, err = f.bufPool.GetPage(f, iterPageNo, tid, ReadPerm)
			if err != nil {
				fmt.Println("Get Page Error in Iterator")
				return nil, nil
			}
			var curPage *heapPage = (*rawPage).(*heapPage)
			curPageIter = curPage.tupleIter()
			tup, _ = curPageIter()
		}
		return tup, nil
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{FileName: f.file, PageNo: pgNo}
}
