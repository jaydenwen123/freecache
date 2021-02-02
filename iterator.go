package freecache

import (
	"unsafe"
)

// Iterator iterates the entries for the cache.
type Iterator struct {
	cache      *Cache
	segmentIdx int
	slotIdx    int
	entryIdx   int
}

// Entry represents a key/value pair.
type Entry struct {
	Key   []byte
	Value []byte
}

// Next returns the next entry for the iterator.
// The order of the entries is not guaranteed.
// If there is no more entries to return, nil will be returned.
func (it *Iterator) Next() *Entry {
	// 循环遍历读取256个segment中的数据
	for it.segmentIdx < 256 {
		entry := it.nextForSegment(it.segmentIdx)
		if entry != nil {
			return entry
		}
		it.segmentIdx++
		it.slotIdx = 0
		it.entryIdx = 0
	}
	return nil
}

func (it *Iterator) nextForSegment(segIdx int) *Entry {
	it.cache.locks[segIdx].Lock()
	defer it.cache.locks[segIdx].Unlock()
	// 找到该segment
	seg := &it.cache.segments[segIdx]
	// 每个segment中有256个slot，因此内部循环读取该segment中的256个slot数据
	for it.slotIdx < 256 {
		entry := it.nextForSlot(seg, it.slotIdx)
		if entry != nil {
			return entry
		}
		it.slotIdx++
		it.entryIdx = 0
	}
	return nil
}

func (it *Iterator) nextForSlot(seg *segment, slotId int) *Entry {
	slotOff := int32(it.slotIdx) * seg.slotCap
	// 该取得slot的数据
	slot := seg.slotsData[slotOff : slotOff+seg.slotLens[it.slotIdx] : slotOff+seg.slotCap]
	// 遍历该slot中的entry
	for it.entryIdx < len(slot) {
		// 取得该slot中的entryIdx元素
		ptr := slot[it.entryIdx]
		it.entryIdx++
		now := seg.timer.Now()
		// 先读24字节的entry头部
		var hdrBuf [ENTRY_HDR_SIZE]byte
		seg.rb.ReadAt(hdrBuf[:], ptr.offset)
		hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
		// 未设置过期时间或者该entry还未过期
		if hdr.expireAt == 0 || hdr.expireAt > now {
			entry := new(Entry)
			entry.Key = make([]byte, hdr.keyLen)
			entry.Value = make([]byte, hdr.valLen)
			// entry_hdr_size(24)+key(bytet[])+value(byte[])
			// 读key
			seg.rb.ReadAt(entry.Key, ptr.offset+ENTRY_HDR_SIZE)
			// 读value
			seg.rb.ReadAt(entry.Value, ptr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
			return entry
		}
	}
	return nil
}

// NewIterator creates a new iterator for the cache.
func (cache *Cache) NewIterator() *Iterator {
	return &Iterator{
		cache: cache,
	}
}
