package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	// "fmt"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	slots           []*Tuple
	unoccupiedSlots map[int]bool
	remPageSize     int32
	remSlots        int32
	totalSlots      int32
	bytespertuple   int32
	dirty           bool
	heapFile        *HeapFile
	pageNo          int
	desc            *TupleDesc
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	// calculate size of each tuple
	var bytespertuple int32
	var remPageSize int32
	// bytespertuple = int32(unsafe.Sizeof(desc))
	bytespertuple = int32(StringLength + int(unsafe.Sizeof(int64(0))))
	remPageSize = int32(PageSize - 8) // increment for header
	totalSlots := int32(remPageSize / bytespertuple)
	slots := make([]*Tuple, totalSlots)
	var unoccupiedSlots = make(map[int]bool)
	for i := 0; i < len(slots); i++ {
		unoccupiedSlots[i] = true
	}
	h := heapPage{slots: slots, unoccupiedSlots: unoccupiedSlots,
				  remPageSize: remPageSize, remSlots: totalSlots,
				  totalSlots: totalSlots, bytespertuple: bytespertuple,
				  dirty: false, heapFile: f, pageNo: pageNo, desc: desc}
	// newPagePointer := (Page)(&h)
	// f.flushPage(&newPagePointer)
	return &h //replace me
}

func (h *heapPage) getNumSlots() int {
	return int(h.remSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.remSlots == 0 {
		return 0, errors.New("no remaining free slots")
	}
	// insert into free slot
	for i := 0; i < int(h.totalSlots); i++ {
		if h.slots[i] == nil {
			t.Rid = RID{pageNo: h.pageNo, slotNum: i}
			h.slots[i] = t
			delete(h.unoccupiedSlots, i)
			break
		}
	}
	// increment remaining slots/memory
	h.remSlots--
	h.remPageSize -= h.bytespertuple
	return t.Rid, nil //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	heapRid, _ := rid.(RID)
	// check if slot is valid/non-empty
	if heapRid.slotNum < 0 || heapRid.slotNum >= int(h.totalSlots) || h.slots[heapRid.slotNum] == nil {
		return errors.New("invalid slot bumber for rid")
	}
	h.slots[heapRid.slotNum] = nil
	h.unoccupiedSlots[heapRid.slotNum] = true
	// increment remaining slots/memory
	h.remSlots++
	h.remPageSize += h.bytespertuple
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var f DBFile = p.heapFile
	return &f
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// write header first
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, h.totalSlots)
	err1 := binary.Write(buf, binary.LittleEndian, h.totalSlots-h.remSlots)
	if err != nil {
		return nil, err
	}
	if err1 != nil {
		return nil, err1
	}
	// write rest of values
	slotNum := 0
	for i := 0; i < len(h.slots); i++ {
		if h.slots[i] != nil {
			err := h.slots[i].writeTo(buf)
			if err != nil {
				return nil, err
			}
			slotNum++
		}
	}
	retBuff := make([]byte, PageSize);
	copy(retBuff, buf.Bytes());
	return bytes.NewBuffer(retBuff), nil;
	// remPageSize := PageSize - 8 - slotNum * int(h.bytespertuple)
	// binary.Write(buf, binary.LittleEndian, make([]byte, remPageSize))
	// return buf, err //replace me

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// create new page and read header
	var totalSlots, usedSlots int32
	// read header
	err := binary.Read(buf, binary.LittleEndian, &totalSlots)
	err1 := binary.Read(buf, binary.LittleEndian, &usedSlots)
	if err != nil || err1 != nil {
		if err != nil {
			return err
		}
		return err1
	}
	// read tuples
	newSlots := make([]*Tuple, totalSlots)
	newUnoccupiedSlots := make(map[int]bool)
	for i := 0; i < int(totalSlots); i++ {
		if i < int(usedSlots) {
			elem, err := readTupleFrom(buf, h.desc)
			if err != nil {
				return err
			}
			elem.Rid = RID{pageNo: h.pageNo, slotNum: i}
			newSlots[i] = elem
		} else {
			newUnoccupiedSlots[i] = true
		}
	}
	// set params to newPage
	bytespertuple := int32(StringLength + int(unsafe.Sizeof(int64(0))))
	h.totalSlots = totalSlots
	h.remSlots = totalSlots - usedSlots
	h.unoccupiedSlots = newUnoccupiedSlots
	// h.remPageSize = int32(PageSize-8) - h.remSlots*bytespertuple
	h.remPageSize = h.remSlots * bytespertuple;
	h.slots = newSlots
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	n := 0
	return func() (*Tuple, error) {
		if n >= int(p.totalSlots) {
			return nil, nil
		}
		// increment until we get to non-empty slot
		for p.slots[n] == nil {
			n += 1
			if n >= int(p.totalSlots) {
				return nil, nil
			}
		}
		val := p.slots[n]
		n++
		return val, nil
	}
}
