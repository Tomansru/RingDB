package vlog

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// IEC Sizes.
const (
	Byte = 1 << (iota * 10) // 2 ^ (iota(0) * 10)
	KiByte
	MiByte
	GiByte
	TiByte
	PiByte
	EiByte
)

const (
	headerSize    = 16 // 4 (keyLen) + 4 (valLen) + 8 (timestamp)
	syncThreshold = 64 * MiByte
)

type buffer []byte

var _pool = sync.Pool{}

func _get(size int) buffer {
	b := _pool.Get()
	if b == nil {
		return make(buffer, size)
	}
	buf := b.(buffer)
	if cap(buf) < size {
		return make(buffer, size)
	}
	return buf[:size]
}

func (b buffer) free() {
	_pool.Put(b[:0])
}

type Vlog struct {
	file *os.File
	data []byte
	size int
	off  int // current write position
	tail int // position of oldest record
	mu   sync.Mutex

	bytesWritten int

	metrics Metrics
}

func NewVlog(path string, size int) (*Vlog, error) {
	// TODO: I want to have minimal available size for the vlog.
	if size <= 124*MiByte {
		return nil, errors.New("vlog size must be greater than 124 MiB")
	}

	err := createDir(path)
	if err != nil {
		return nil, err
	}

	path = filepath.Join(path, "ring.vlog")

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// TODO Case 1: We changed the size
	//   - If the file size is less, we need to extend it. Profit++
	//   - If the file size is more, we need to ???
	//     Problem is that latest date may be lost on the end of the file.
	//   For now we just skip resizing the file.
	fileSize := fi.Size()
	if fileSize == 0 {
		if err := f.Truncate(int64(size)); err != nil {
			_ = f.Close()
			return nil, err
		}
		fileSize = int64(size)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// TODO Case 2: We should reindex the offset
	//   It is only a data store, we should read it all and find the last offset.
	//   Or we can store the offset and index dump in the file nearby or in the header.
	//   For now we just start from the beginning.

	v := &Vlog{
		file: f,
		data: data,
		size: int(fileSize),
		off:  0,
		tail: 0,
	}

	// Just initialize metrics
	v.metrics.OldestTimestamp = time.Now()

	return v, nil
}

// Write writes a record with the given key and value
// returns the overwritten keys and the record offset
func (v *Vlog) Write(key, value []byte) (overwrittenKeys [][]byte, recordOffset int) {
	v.mu.Lock()
	defer v.mu.Unlock()

	totalSize := headerSize + len(key) + len(value)
	free := v.freeSpace()
	tail := v.tail

	// if not enough space, then move tail forward
	for free < totalSize {
		next, k, ts := v.readRawKey(tail)
		overwrittenKeys = append(overwrittenKeys, k)
		freed := (next - tail + v.size) % v.size
		free += freed
		tail = next

		v.metrics.BytesOverwritten += uint64(freed)
		v.metrics.ObjectsOverwritten++
		v.metrics.ObjectsCount--
		v.metrics.OldestTimestamp = time.UnixMilli(ts)
	}

	recordOffset, nextOff := v.writeRaw(v.off, key, value)
	v.tail = tail
	v.off = nextOff

	v.metrics.BytesWritten += uint64(totalSize)
	v.metrics.ObjectsWritten++
	v.metrics.ObjectsCount++
	return overwrittenKeys, recordOffset
}

// freeSpace calculates the free space between the current write offset and the tail
func (v *Vlog) freeSpace() int {
	// If it is fresh start (ObjectsCount == 0), we will have all space available
	if v.metrics.ObjectsWritten != 0 && v.off == v.tail {
		return 0
	}
	if v.off >= v.tail {
		return v.size - (v.off - v.tail)
	}
	return v.tail - v.off
}

// writeRawMMap writes a record at the given offset
// returns the record offset and next write offset
func (v *Vlog) writeRawMMap(off int, key, value []byte) (int, int) {
	recordOffset := off
	totalSize := headerSize + len(key) + len(value)
	end := off + totalSize
	ts := time.Now().UnixMilli()

	v.bytesWritten += totalSize

	if end <= v.size {
		binary.BigEndian.PutUint32(v.data[off:], uint32(len(key)))
		binary.BigEndian.PutUint32(v.data[off+4:], uint32(len(value)))
		binary.BigEndian.PutUint64(v.data[off+8:], uint64(ts))
		copy(v.data[off+headerSize:], key)
		copy(v.data[off+headerSize+len(key):], value)

		if v.bytesWritten >= syncThreshold {
			err := unix.Msync(v.data, unix.MS_ASYNC)
			assert(err)
			v.bytesWritten = 0
		}

		return recordOffset, end % v.size
	}

	v.metrics.OverwriteCycles++

	remain := v.size - off
	buf := _get(totalSize)
	defer buf.free()
	binary.BigEndian.PutUint32(buf, uint32(len(key)))
	binary.BigEndian.PutUint32(buf[4:], uint32(len(value)))
	binary.BigEndian.PutUint64(buf[8:], uint64(ts))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)

	copy(v.data[off:], buf[:remain])
	copy(v.data, buf[remain:])

	if v.bytesWritten >= syncThreshold {
		err := unix.Msync(v.data, unix.MS_ASYNC)
		assert(err)
		v.bytesWritten = 0
	}

	return recordOffset, end % v.size
}

// writeRaw writes a record at the given offset
// returns the record offset and next write offset
func (v *Vlog) writeRaw(off int, key, value []byte) (int, int) {
	recordOffset := off
	totalSize := headerSize + len(key) + len(value)
	end := off + totalSize
	ts := time.Now().UnixMilli()

	buf := _get(totalSize)
	defer buf.free()
	binary.BigEndian.PutUint32(buf[0:], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[4:], uint32(len(value)))
	binary.BigEndian.PutUint64(buf[8:], uint64(ts))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)

	v.bytesWritten += totalSize

	if end <= v.size {
		_, err := v.file.WriteAt(buf, int64(off))
		assert(err)

		if v.bytesWritten >= syncThreshold {
			err = v.file.Sync()
			assert(err)
			v.bytesWritten = 0
		}

		return recordOffset, end % v.size
	}

	v.metrics.OverwriteCycles++

	remain := v.size - off
	_, err := v.file.WriteAt(buf[:remain], int64(off))
	assert(err)
	_, err = v.file.WriteAt(buf[remain:], 0)
	assert(err)

	if v.bytesWritten >= syncThreshold {
		err = v.file.Sync()
		assert(err)
		v.bytesWritten = 0
	}

	return recordOffset, end % v.size
}

// Read reads a record at the given offset
// returns the next record offset, key, and value
func (v *Vlog) Read(offset int) (nextOffset int, key, value []byte) {
	v.mu.Lock()
	defer v.mu.Unlock()
	nextOffset, key, value = v.readRaw(offset)
	v.metrics.BytesRead += uint64(headerSize + len(key) + len(value))
	v.metrics.ObjectsRead++
	return nextOffset, key, value
}

// readRaw reads a record at the given offset
// returns the next record offset, key, and value
func (v *Vlog) readRaw(off int) (int, []byte, []byte) {
	header := _get(headerSize)
	defer header.free()

	if off+headerSize <= v.size {
		copy(header, v.data[off:off+headerSize])
	} else {
		remain := v.size - off
		copy(header, v.data[off:])
		copy(header[remain:], v.data[:headerSize-remain])
	}

	keyLen := int(binary.BigEndian.Uint32(header[0:4]))
	dataLen := int(binary.BigEndian.Uint32(header[4:8]))
	_ = int64(binary.BigEndian.Uint64(header[8:16]))
	dataSize := keyLen + dataLen
	totalSize := headerSize + dataSize

	// TODO I think it is good place for bufferPool
	//   I think we will use marshaling of data higher up the stack
	//   and we can use buffer pool to avoid allocations.

	buf := make([]byte, dataSize)
	dataOff := (off + headerSize) % v.size
	if dataOff+dataSize <= v.size {
		copy(buf, v.data[dataOff:dataOff+dataSize])
	} else {
		remain := v.size - dataOff
		copy(buf, v.data[dataOff:])
		copy(buf[remain:], v.data[:dataSize-remain])
	}

	key := buf[:keyLen]
	value := buf[keyLen:]
	nextOff := (off + totalSize) % v.size
	return nextOff, key, value
}

// readRawKey same as readRaw, but only reads header and the key
// returns the next record offset, the key and record timestamp in milliseconds
func (v *Vlog) readRawKey(off int) (int, []byte, int64) {
	header := _get(headerSize)
	defer header.free()

	if off+headerSize <= v.size {
		copy(header, v.data[off:off+headerSize])
	} else {
		remain := v.size - off
		copy(header, v.data[off:])
		copy(header[remain:], v.data[:headerSize-remain])
	}

	keyLen := int(binary.BigEndian.Uint32(header[0:4]))
	dataLen := int(binary.BigEndian.Uint32(header[4:8]))
	timestamp := int64(binary.BigEndian.Uint64(header[8:16]))
	dataSize := keyLen + dataLen
	totalSize := headerSize + dataSize

	key := make([]byte, keyLen)
	dataOff := (off + headerSize) % v.size
	if dataOff+keyLen <= v.size {
		copy(key, v.data[dataOff:dataOff+keyLen])
	} else {
		remain := v.size - dataOff
		copy(key, v.data[dataOff:])
		copy(key[remain:], v.data[:keyLen-remain])
	}

	nextOff := (off + totalSize) % v.size
	return nextOff, key, timestamp
}

func (v *Vlog) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := syscall.Munmap(v.data); err != nil {
		return err
	}
	return v.file.Close()
}

type Metrics struct {
	BytesWritten   uint64
	ObjectsWritten uint64

	BytesRead   uint64
	ObjectsRead uint64

	BytesOverwritten   uint64
	ObjectsOverwritten uint64
	OverwriteCycles    uint64

	OldestTimestamp time.Time
	ObjectsCount    uint64
}

func (v *Vlog) Metrics() Metrics {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.metrics
}

func createDir(path string) error {
	dirExists, err := exists(path)
	if err != nil {
		return err
	}
	if !dirExists {
		err = os.MkdirAll(path, 0700)
		if err != nil {
			return err
		}
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func assert(err error) {
	if err != nil {
		panic(err)
	}
}
