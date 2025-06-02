package ringdb

import (
	"errors"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/google/btree"

	"btree/pkg/vlog"
)

type RingDB struct {
	btree *btree.BTreeG[*BTreeEntity]
	vlog  *vlog.Vlog
	mu    sync.Mutex

	rate *time.Ticker
}

// NewRingDB creates a new RingDB instance.
// IMPORTANT: For now we store btree in memory, so it can grow indefinitely.
// Be careful with the size of RAM and count of records.
//
// Parameters:
//   - filePath: path to the vlog file
//   - maxSize: maximum size of the vlog file in bytes
//   - degree: degree of the B-tree (number of children per node)
//   - rate: minimum duration between Upsert operations, typical 500*time.Microsecond = 2000 rps. (0 means no rate limit)
//     Sometimes mmap have sync lags on high write load, so we use ticker to limit the rate of Upsert operations.
//     TODO Probably it depends from bytes written to the vlog file, not from rps
//
// You should call Close() method to release resources when you are done with the RingDB instance.
func NewRingDB(filePath string, maxSize, degree int, rate time.Duration) (*RingDB, error) {
	vl, err := vlog.NewVlog(filePath, maxSize)
	if err != nil {
		return nil, err
	}

	// degree 64: ~~~ 4096 B / 30 (sizeof(BTreeEntity))
	if degree == 0 {
		degree = 64 // Default degree
	}

	b := btree.NewG[*BTreeEntity](degree, less)

	r := &RingDB{
		btree: b,
		vlog:  vl,
	}

	if rate > 0 {
		r.rate = time.NewTicker(rate)
	}

	return r, nil
}

func (r *RingDB) Close() error {
	if r.rate != nil {
		r.rate.Stop()
	}

	if r.vlog != nil {
		return r.vlog.Close()
	}

	return nil
}

type BTreeEntity struct {
	Key          string
	RecordOffset int
}

func less(a, b *BTreeEntity) bool {
	return a.Key < b.Key
}

type Entity struct {
	Key   string
	Value []byte
}

var ErrNilEntity = errors.New("entity cannot be nil")

func (r *RingDB) Upsert(e *Entity) error {
	if e == nil {
		return ErrNilEntity
	}

	<-r.rate.C

	r.mu.Lock()
	defer r.mu.Unlock()

	keyBytes := stringToBytes(e.Key)
	keys, offset := r.vlog.Write(keyBytes, e.Value)

	// If keys are returned, it means the key was rewritten
	if len(keys) > 0 {
		for _, key := range keys {
			// TODO BUG If we have old and new version of the key,
			//   we will delete new version of the key while deleting old version.
			//   We need to separate different versions of the key.
			//   Maybe we can use a timestamp like versioning.
			_, deleted := r.btree.Delete(&BTreeEntity{Key: string(key)})
			if !deleted {
				return errors.New("IMPOSSIBLE BUG failed to delete old key from btree")
			}
		}
	}

	_, _ = r.btree.ReplaceOrInsert(&BTreeEntity{
		Key:          e.Key,
		RecordOffset: offset,
	})

	return nil
}

var errKeyMismatch = errors.New("IMPOSSIBLE BUG key mismatch or not found in vlog")

func (r *RingDB) Read(key string) (*Entity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	item, found := r.btree.Get(&BTreeEntity{Key: key})
	if !found {
		return nil, nil
	}

	_, bkey, value := r.vlog.Read(item.RecordOffset)

	if bkey == nil || len(bkey) == 0 || key != bytesToString(bkey) {
		return nil, errKeyMismatch
	}

	return &Entity{
		Key:   bytesToString(bkey),
		Value: value,
	}, nil
}

func (r *RingDB) ReadPrefix(prefix string) ([]*Entity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	var entities []*Entity
	r.btree.AscendGreaterOrEqual(&BTreeEntity{Key: prefix}, func(item *BTreeEntity) bool {
		if !strings.HasPrefix(item.Key, prefix) {
			return false
		}

		_, bkey, value := r.vlog.Read(item.RecordOffset)

		if bkey == nil || len(bkey) == 0 || !strings.HasPrefix(bytesToString(bkey), prefix) {
			err = errKeyMismatch
			return false
		}

		entities = append(entities, &Entity{
			Key:   bytesToString(bkey),
			Value: value,
		})

		return true
	})

	return entities, err
}

func (r *RingDB) VlogMetrics() vlog.Metrics {
	if r.vlog == nil {
		return vlog.Metrics{}
	}
	return r.vlog.Metrics()
}

func stringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
