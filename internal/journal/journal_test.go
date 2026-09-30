package journal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRoundTripAndTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.bin")
	w, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	in := []Record{
		{Symbol: "BTC-USDT", Op: 1, Side: 1, Kind: 1, ID: 1, Price: 100, Qty: 5, TS: 1},
		{Symbol: "ETH-USDT", Op: 2, ID: 1, TS: 2},
	}
	if err := w.Append(in); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// simulate a crash in the middle of the next append
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0x30, 0, 0, 0, 1, 2})
	f.Close()

	var out []Record
	if err := Replay(path, func(r Record) error { out = append(out, r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0] != in[0] || out[1] != in[1] {
		t.Fatalf("replay mismatch: %+v", out)
	}
	st, _ := os.Stat(path)
	if want := int64(len(encode(nil, in[0])) + len(encode(nil, in[1]))); st.Size() != want {
		t.Fatalf("torn tail not truncated: size=%d want=%d", st.Size(), want)
	}
}
