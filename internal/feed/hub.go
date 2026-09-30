// Package feed fans trades out to Server-Sent Events subscribers.
//
// Publishing never blocks the matching loop: every subscriber has a bounded
// buffer and is disconnected when it falls behind.
package feed

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/YoungOver/orderbook-engine/internal/book"
)

type event struct {
	Symbol string       `json:"symbol"`
	Trades []book.Trade `json:"trades"`
}

type sub struct {
	symbol string
	ch     chan []byte
}

type Hub struct {
	mu      sync.RWMutex
	subs    map[*sub]struct{}
	Dropped atomic.Uint64
	buf     int
}

func NewHub(buffer int) *Hub { return &Hub{subs: map[*sub]struct{}{}, buf: buffer} }

func (h *Hub) Publish(symbol string, trades []book.Trade) {
	h.mu.RLock()
	if len(h.subs) == 0 {
		h.mu.RUnlock()
		return
	}
	msg, _ := json.Marshal(event{Symbol: symbol, Trades: trades})
	var slow []*sub
	for s := range h.subs {
		if s.symbol != "" && s.symbol != symbol {
			continue
		}
		select {
		case s.ch <- msg:
		default:
			slow = append(slow, s)
		}
	}
	h.mu.RUnlock()
	if len(slow) > 0 {
		h.mu.Lock()
		for _, s := range slow {
			if _, ok := h.subs[s]; ok {
				delete(h.subs, s)
				close(s.ch)
				h.Dropped.Add(1)
			}
		}
		h.mu.Unlock()
	}
}

func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// ServeHTTP streams trades as text/event-stream. ?symbol= filters one book.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	s := &sub{symbol: r.URL.Query().Get("symbol"), ch: make(chan []byte, h.buf)}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if _, ok := h.subs[s]; ok {
			delete(h.subs, s)
			close(s.ch)
		}
		h.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-s.ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: trades\ndata: %s\n\n", msg)
			fl.Flush()
		}
	}
}
