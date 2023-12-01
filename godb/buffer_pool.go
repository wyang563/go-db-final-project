package godb

import (
	"fmt"
	"errors"
	"sync"
	"time"
	// "math/rand"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type lock struct {
	page		heapHash
	lockType	RWPerm  
}

type BufferPool struct {
	// TODO: some code goes here
	PageNoToPage	map[heapHash]*Page
	curPages		int
	limit 			int
	m				sync.Mutex
	writeLocks		map[heapHash]TransactionID
	readLocks		map[heapHash](map[TransactionID]bool)
	tidToLocks		map[TransactionID]([]lock)
	conflictGraph	map[TransactionID](map[TransactionID]bool)	
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	return &BufferPool{limit: numPages, PageNoToPage: make(map[heapHash]*Page), curPages: 0, 
		               writeLocks: make(map[heapHash]TransactionID), readLocks: make(map[heapHash](map[TransactionID]bool)),
					   tidToLocks: make(map[TransactionID][]lock), conflictGraph: make(map[TransactionID](map[TransactionID]bool))};
}

// function for debugging 
func (bp *BufferPool) PrintParams(tid TransactionID) {
	fmt.Println("####### TID - ", tid);
	fmt.Println("WRITE LOCKS - ", bp.writeLocks);
	fmt.Println("READ LOCKS - ", bp.readLocks);
	fmt.Println("TID LOCKS - ", bp.tidToLocks);
	fmt.Println("CONFLICT MAP - ", bp.conflictGraph);
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.PageNoToPage {
		file := *((*(page)).getFile());
		file.flushPage(page);
	}
}

// DFS to check for deadlocks
func (bp *BufferPool) checkDeadlocks(tid TransactionID) bool {
	// fmt.Println("CHECKING DEADLOCK");
	// fmt.Println("DEADLOCK GRAPH - ", bp.conflictGraph);
	bp.m.Lock();
	startNode := tid;
	queue := make([]TransactionID, 0);
	queue = append(queue, startNode);
	visited := make(map[TransactionID]bool);
	cycleExists := false;
	for len(queue) > 0 {
		cur := queue[len(queue) - 1];
		queue = queue[:len(queue)-1];
		visited[cur] = true;
		for neighbor, _ := range bp.conflictGraph[cur] {
			_, ok := visited[neighbor];
			if !ok {
				queue = append(queue, neighbor);
			}
			// if we return to startNode there exists a cycle
			if neighbor == startNode {
				cycleExists = true;
				break;
			}
		}
	}
	bp.m.Unlock();
	// fmt.Println("RESULT  - ", cycleExists);	
	return cycleExists;
}

func copyMap(inputMap map[TransactionID]bool) map[TransactionID]bool {
	copyMap := make(map[TransactionID]bool);
	for k, v := range inputMap {
		copyMap[k] = v;
	}
	return copyMap;
}

// Removes all dependencies once a transaction releases a given tid
func (bp *BufferPool) removeDependencies(tid TransactionID) {
	// remove all edges going to tid
	for key, conflictList := range bp.conflictGraph {
		newConflictList := make(map[TransactionID]bool);
		for elem, _ := range conflictList {
			if elem != tid {
				newConflictList[elem] = true;
			}
		}
		bp.conflictGraph[key] = copyMap(newConflictList);
	}
	// remove all edges going from tid
	delete(bp.conflictGraph, tid);
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.m.Lock();
	// fmt.Println("ABORTING TID - ", tid);
	bp.removeDependencies(tid);
	for _, lockVal := range bp.tidToLocks[tid] {
		// delete tid in readLocks
		_, ok := bp.readLocks[lockVal.page][tid];
		if ok {
			delete(bp.readLocks[lockVal.page], tid);
		}
		// delete tid in writeLocks
		if bp.writeLocks[lockVal.page] == tid {
			delete(bp.writeLocks, lockVal.page);
		}
		delete(bp.PageNoToPage, lockVal.page);
		bp.curPages--;
	}
	delete(bp.tidToLocks, tid);
	// fmt.Println("ABORTED TID - ", tid);
	bp.m.Unlock();
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// fmt.Println("COMMITING TID - ", tid);
	bp.m.Lock();
	bp.removeDependencies(tid);
	for _, lockVal := range bp.tidToLocks[tid] {
		// delete tid in readLocks
		_, ok := bp.readLocks[lockVal.page][tid];
		if ok {
			delete(bp.readLocks[lockVal.page], tid);
		}
		// delete tid in writeLocks
		if bp.writeLocks[lockVal.page] == tid {
			delete(bp.writeLocks, lockVal.page);
		}
		// write page to file if dirty
		_, ok = bp.PageNoToPage[lockVal.page];
		if ok {
			page := *(bp.PageNoToPage[lockVal.page]);
			if page.isDirty() {
				file := *(page.getFile());
				file.flushPage(&page);
				delete(bp.PageNoToPage, lockVal.page);
				bp.curPages--;
			}
		}
	}
	delete(bp.tidToLocks, tid);
	// fmt.Println("COMMITED TID - ", tid);
	bp.m.Unlock();
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Checks if another tid process is writing to the same pk heap file
func (bp *BufferPool) checkWriting(pk heapHash, tid TransactionID) bool {
	bp.m.Lock();
	tidVal, ok := bp.writeLocks[pk];
	conflict := false;
	if (ok && tidVal != tid) {
		// update conflict graph 
		if bp.conflictGraph[tid] == nil {
			bp.conflictGraph[tid] = make(map[TransactionID]bool);
		}
		bp.conflictGraph[tid][tidVal] = true;
		conflict = true;
	}
	bp.m.Unlock();
	return conflict;
}

// Checks if another tid process is reading to the same pk heap file
func (bp *BufferPool) checkReading(pk heapHash, tid TransactionID) bool {
	bp.m.Lock();
	tidList, ok := bp.readLocks[pk];
	conflict := false;
	if ok {
		for tidVal := range tidList {
			if tidVal != tid {
				conflict = true;
				// update conflict graph
				if bp.conflictGraph[tid] == nil {
					bp.conflictGraph[tid] = make(map[TransactionID]bool);
				}
				bp.conflictGraph[tid][tidVal] = true;
			}
		}
	}
	bp.m.Unlock();
	return conflict;
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk using [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	pk := file.pageKey(pageNo).(heapHash);
	// acquire locks
	bp.m.Lock();

	// check if tid already has a lock
	hasLock := false;
	if bp.tidToLocks[tid] != nil {
		for _, lock := range bp.tidToLocks[tid] {
			if lock.lockType == perm && lock.page == pk {
				hasLock = true;
				break;
			}
		}
	}
	if !hasLock {
		if (perm == ReadPerm) {
			// check if something is writing and block 
			bp.m.Unlock();
			for bp.checkWriting(pk, tid) {
				// check for deadlocks
				time.Sleep(14 * time.Millisecond);
				if bp.checkDeadlocks(tid) {
					bp.AbortTransaction(tid);
					return nil, nil;
				}
			}
			bp.m.Lock();
			_, ok := bp.readLocks[pk];
			if !ok {
				bp.readLocks[pk] = make(map[TransactionID]bool);
			}
			bp.readLocks[pk][tid] = true;
		} else {
			// check if page is being written by another process
			bp.m.Unlock();
			for (bp.checkWriting(pk, tid) || bp.checkReading(pk, tid)) {
				time.Sleep(14 * time.Millisecond);
				if bp.checkDeadlocks(tid) {
					bp.AbortTransaction(tid);
					return nil, fmt.Errorf("deadlock detected aborting transaction");
				}
			}
			bp.m.Lock();
			// update vals
			bp.writeLocks[pk] = tid;
		}
		// update tidToLocks map
		_, ok := bp.tidToLocks[tid];
		if !ok {
			bp.tidToLocks[tid] = make([]lock, 0);
		}
		bp.tidToLocks[tid] = append(bp.tidToLocks[tid], lock{page: pk, lockType: perm});
	}

	bp.m.Unlock();
	// check whether page is in buffer
	page, ok := bp.PageNoToPage[pk];
	if ok {
		// return page if it's in cache
		return page, nil
	}
	bp.m.Lock();
	defer bp.m.Unlock();
	// otherwise read page from memory
	pageFromMem, err := file.readPage(pageNo)
	if err != nil {
		// fmt.Println("Error Reading Page:", pageNo);
		return nil, err
	}
	// check if buffer pool is full, if not add it to buffer pool
	if bp.curPages < bp.limit {
		bp.curPages++
		bp.PageNoToPage[pk] = pageFromMem
	} else {
		hasCleanPage := false
		for tempPageNo, tempPage := range bp.PageNoToPage {
			if ! ((*tempPage).isDirty()) {
				hasCleanPage = true
				// flush page from heapFile before deleting it
				file.flushPage(tempPage)
				(*tempPage).setDirty(false);
				delete(bp.PageNoToPage, tempPageNo)
				bp.PageNoToPage[pk] = pageFromMem
				break;
			}
		}
		if !hasCleanPage {
			return nil, errors.New("no clean pages in cache")
		}
	}
	return pageFromMem, err
}
