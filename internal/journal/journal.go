// Package journal is an append-only binary command log.
//
// Every record is length-prefixed and CRC32-protected. A torn write at the end
// of the file (power loss mid-append) is detected on replay and truncated.
package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type Record struct {
	Symbol string
	Op     uint8
	Side   uint8
	Kind   uint8
	ID     uint64
	Price  int64
	Qty    int64
	TS     int64
}

var table = crc32.MakeTable(crc32.Castagnoli)

// fixed part: op, side, kind (3) + id, price, qty, ts (32) + symbol length (1)
const fixed = 3 + 32 + 1

func encode(dst []byte, r Record) []byte {
	body := make([]byte, 0, fixed+len(r.Symbol))
	body = append(body, r.Op, r.Side, r.Kind)
	body = binary.LittleEndian.AppendUint64(body, r.ID)
	body = binary.LittleEndian.AppendUint64(body, uint64(r.Price))
	body = binary.LittleEndian.AppendUint64(body, uint64(r.Qty))
	body = binary.LittleEndian.AppendUint64(body, uint64(r.TS))
	body = append(body, byte(len(r.Symbol)))
	body = append(body, r.Symbol...)

	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(body)))
	dst = binary.LittleEndian.AppendUint32(dst, crc32.Checksum(body, table))
	return append(dst, body...)
}

type Writer struct {
	mu    sync.Mutex
	f     *os.File
	buf   []byte
	fsync bool
}

func Open(path string, fsync bool) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, fsync: fsync}, nil
}

// Append writes a batch with a single syscall and an optional fsync.
func (w *Writer) Append(rs []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = w.buf[:0]
	for _, r := range rs {
		w.buf = encode(w.buf, r)
	}
	if _, err := w.f.Write(w.buf); err != nil {
		return err
	}
	if w.fsync {
		return w.f.Sync()
	}
	return nil
}

func (w *Writer) Close() error { return w.f.Close() }

var errCorrupt = errors.New("journal: corrupt record")

// Replay calls fn for every valid record and truncates a damaged tail.
func Replay(path string, fn func(Record) error) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if err == io.EOF {
				return nil
			}
			return f.Truncate(off)
		}
		n := binary.LittleEndian.Uint32(hdr)
		sum := binary.LittleEndian.Uint32(hdr[4:])
		if n < fixed || n > 1<<16 {
			return f.Truncate(off)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil || crc32.Checksum(body, table) != sum {
			return f.Truncate(off)
		}
		rec, err := decode(body)
		if err != nil {
			return f.Truncate(off)
		}
		if err := fn(rec); err != nil {
			return err
		}
		off += int64(8 + n)
	}
}

func decode(b []byte) (Record, error) {
	if len(b) < fixed {
		return Record{}, errCorrupt
	}
	r := Record{Op: b[0], Side: b[1], Kind: b[2]}
	r.ID = binary.LittleEndian.Uint64(b[3:])
	r.Price = int64(binary.LittleEndian.Uint64(b[11:]))
	r.Qty = int64(binary.LittleEndian.Uint64(b[19:]))
	r.TS = int64(binary.LittleEndian.Uint64(b[27:]))
	sl := int(b[35])
	if len(b) < fixed+sl {
		return Record{}, errCorrupt
	}
	r.Symbol = string(b[fixed : fixed+sl])
	return r, nil
}
