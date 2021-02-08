package freecache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var ErrOutOfRange = errors.New("out of range")

// Ring buffer has a fixed size, when data exceeds the
// size, old data will be overwritten by new data.
// It only contains the data in the stream from begin to end
type RingBuf struct {
	begin int64 // beginning offset of the data stream.
	end   int64 // ending offset of the data stream.
	data  []byte
	index int // range from '0' to 'len(rb.data)-1'
}

func NewRingBuf(size int, begin int64) (rb RingBuf) {
	// 分配固定的空间
	rb.data = make([]byte, size)
	rb.Reset(begin)
	return
}

// Reset the ring buffer
//
// Parameters:
//     begin: beginning offset of the data stream
func (rb *RingBuf) Reset(begin int64) {
	rb.begin = begin
	rb.end = begin
	rb.index = 0
}

// Create a copy of the buffer.
func (rb *RingBuf) Dump() []byte {
	dump := make([]byte, len(rb.data))
	copy(dump, rb.data)
	return dump
}

func (rb *RingBuf) String() string {
	return fmt.Sprintf("[size:%v, start:%v, end:%v, index:%v]", len(rb.data), rb.begin, rb.end, rb.index)
}

func (rb *RingBuf) Size() int64 {
	return int64(len(rb.data))
}

func (rb *RingBuf) Begin() int64 {
	return rb.begin
}

func (rb *RingBuf) End() int64 {
	return rb.end
}

// read up to len(p), at off of the data stream.
func (rb *RingBuf) ReadAt(p []byte, off int64) (n int, err error) {
	if off > rb.end || off < rb.begin {
		err = ErrOutOfRange
		return
	}
	// 读的起始下标
	readOff := rb.getDataOff(off)
	// 读的结束下标
	readEnd := readOff + int(rb.end-off)
	// 0<=start<=readOff<=readEnd<=len(data)
	if readEnd <= len(rb.data) {
		n = copy(p, rb.data[readOff:readEnd])
	} else {
		// 大于这个len(data)的话，readOff<=len(data)<=readEnd
		// 分两段来读
		// 第一段readOff~len(data)
		n = copy(p, rb.data[readOff:])
		if n < len(p) {
			// 第二段0~readEnd-len(data)
			// 读了n个字符，所以从n之后开始读取剩下的数据
			n += copy(p[n:], rb.data[:readEnd-len(rb.data)])
		}
	}
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (rb *RingBuf) getDataOff(off int64) int {
	var dataOff int
	// 0<=begin<=end<=len(data)
	if rb.end-rb.begin < int64(len(rb.data)) {
		dataOff = int(off - rb.begin)
	} else {
		// 0<=begin<=len(data)<=end
		// index<begin,off-begin+index
		dataOff = rb.index + int(off-rb.begin)
	}
	if dataOff >= len(rb.data) {
		dataOff -= len(rb.data)
	}
	return dataOff
}

// Slice returns a slice of the supplied range of the ring buffer. It will
// not alloc unless the requested range wraps the ring buffer.
func (rb *RingBuf) Slice(off, length int64) ([]byte, error) {
	if off > rb.end || off < rb.begin {
		return nil, ErrOutOfRange
	}
	readOff := rb.getDataOff(off)
	readEnd := readOff + int(length)
	// 不发生拷贝
	if readEnd <= len(rb.data) {
		return rb.data[readOff:readEnd:readEnd], nil
	}
	buf := make([]byte, length)
	n := copy(buf, rb.data[readOff:])
	if n < int(length) {
		n += copy(buf[n:], rb.data[:readEnd-len(rb.data)])
	}
	if n < int(length) {
		return nil, io.EOF
	}
	return buf, nil
}

func (rb *RingBuf) Write(p []byte) (n int, err error) {
	if len(p) > len(rb.data) {
		err = ErrOutOfRange
		return
	}
	for n < len(p) {
		// 从index开始往后写，end是一直增加的，index即是end超过len(data)后截取的新的写入位置
		written := copy(rb.data[rb.index:], p[n:])
		rb.end += int64(written)
		n += written
		rb.index += written
		// end其实是不变，index是相当于end取余后的数字
		if rb.index >= len(rb.data) {
			rb.index -= len(rb.data)
		}
	}
	// 满了
	if int(rb.end-rb.begin) > len(rb.data) {
		//
		rb.begin = rb.end - int64(len(rb.data))
	}
	return
}

func (rb *RingBuf) WriteAt(p []byte, off int64) (n int, err error) {
	if off+int64(len(p)) > rb.end || off < rb.begin {
		err = ErrOutOfRange
		return
	}
	// 获取写入的位置
	writeOff := rb.getDataOff(off)

	writeEnd := writeOff + int(rb.end-off)

	// 分一段写
	if writeEnd <= len(rb.data) {
		n = copy(rb.data[writeOff:writeEnd], p)
	} else {
		// 分两段写writeOff~size,0~writeEnd-size
		n = copy(rb.data[writeOff:], p)
		if n < len(p) {
			// newEnd=writeEnd-len(rb.data)
			n += copy(rb.data[:writeEnd-len(rb.data)], p[n:])
		}
	}
	return
}

func (rb *RingBuf) EqualAt(p []byte, off int64) bool {
	if off+int64(len(p)) > rb.end || off < rb.begin {
		return false
	}
	readOff := rb.getDataOff(off)
	readEnd := readOff + len(p)
	if readEnd <= len(rb.data) {
		return bytes.Equal(p, rb.data[readOff:readEnd])
	} else {
		// 分两段来比较，先比较第一段
		firstLen := len(rb.data) - readOff
		equal := bytes.Equal(p[:firstLen], rb.data[readOff:])
		// 第一段相等再比较第二段
		if equal {
			secondLen := len(p) - firstLen
			equal = bytes.Equal(p[firstLen:], rb.data[:secondLen])
		}
		return equal
	}
}

// Evacuate read the data at off, then write it to the the data stream,
// Keep it from being overwritten by new data.
// 从off位置开始读取，然后再重新从index写入
func (rb *RingBuf) Evacuate(off int64, length int) (newOff int64) {
	if off+int64(length) > rb.end || off < rb.begin {
		return -1
	}
	readOff := rb.getDataOff(off)
	if readOff == rb.index {
		// no copy evacuate
		// 不需要再写了，但是需要把index移动
		rb.index += length
		if rb.index >= len(rb.data) {
			rb.index -= len(rb.data)
		}
	} else if readOff < rb.index {
		// 先读取，然后再写入到index之后
		var n = copy(rb.data[rb.index:], rb.data[readOff:readOff+length])
		rb.index += n
		if rb.index == len(rb.data) {
			rb.index = copy(rb.data, rb.data[readOff+n:readOff+length])
		}
	} else {
		// readOff>index
		var readEnd = readOff + length
		var n int
		// readeEnd<len(data)
		if readEnd <= len(rb.data) {
			n = copy(rb.data[rb.index:], rb.data[readOff:readEnd])
			rb.index += n
		} else {
			// 拷贝第一段
			n = copy(rb.data[rb.index:], rb.data[readOff:])
			rb.index += n
			// 拷贝剩余的数据
			var tail = length - n
			n = copy(rb.data[rb.index:], rb.data[:tail])
			rb.index += n
			if rb.index == len(rb.data) {
				rb.index = copy(rb.data, rb.data[n:tail])
			}
		}
	}
	newOff = rb.end
	rb.end += int64(length)
	if rb.begin < rb.end-int64(len(rb.data)) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return
}

func (rb *RingBuf) Resize(newSize int) {
	if len(rb.data) == newSize {
		return
	}
	newData := make([]byte, newSize)
	var offset int
	// 满了
	if rb.end-rb.begin == int64(len(rb.data)) {
		//
		offset = rb.index
	}
	// 数据还大，有部分数据要丢弃
	if int(rb.end-rb.begin) > newSize {
		// newSize+1~end
		discard := int(rb.end-rb.begin) - newSize
		// 丢弃的数据等价于写入到offset之后，所以按照下面重新计算offset
		offset = (offset + discard) % len(rb.data)
		rb.begin = rb.end - int64(newSize)
	}
	n := copy(newData, rb.data[offset:])
	if n < newSize {
		copy(newData[n:], rb.data[:offset])
	}
	rb.data = newData
	rb.index = 0
}

func (rb *RingBuf) Skip(length int64) {
	rb.end += length
	rb.index += int(length)
	for rb.index >= len(rb.data) {
		rb.index -= len(rb.data)
	}
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
}
