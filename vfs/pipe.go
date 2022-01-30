package vfs

import (
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
)

/*
	TODO: Limit writes to avoid exploding the memory. Even though we need to support WriteAt, it is just
	      a limitation due to the use of pipeat here are there in the code.
	TODO: Back buffers by files on system to avoid using memory. The original intent of this new BufferedPipe
	      was to not requiring to write the full uploaded file locally before sending it.
*/

type Range struct {
	next       *Range
	start, end int
}

func (r *Range) String() string {
	var s strings.Builder
	for p := r; p != nil; p = p.next {
		s.WriteString(strconv.Itoa(p.start))
		s.WriteString("..")
		s.WriteString(strconv.Itoa(p.end))
		s.WriteString(";")
	}
	return s.String()
}

type Action int

const (
	writeAction Action = iota
	readAction
)

type Buffer struct {
	mu      sync.Mutex
	b       []byte
	written *Range
	read    *Range
}

func (b *Buffer) Filled() bool {
	return b.written != nil && b.written.start == 0 && b.written.end == cap(b.b)-1
}

func (b *Buffer) Emptied() bool {
	return b.read != nil && b.read.start == 0 && b.read.end == cap(b.b)-1
}

func (b *Buffer) UpdateRange(rt Action, start, end int) {
	var cur *Range
	if rt == writeAction {
		if b.written == nil {
			b.written = &Range{start: start, end: end}
			return
		}
		cur = b.written
	} else if rt == readAction {
		if b.read == nil {
			b.read = &Range{start: start, end: end}
			return
		}
		cur = b.read
	}
	if cur == nil {
		return
	}

	for {
		if start > cur.end+1 {
			if cur.next == nil {
				cur.next = &Range{start: start, end: end}
				break
			} else {
				cur = cur.next
				continue
			}
		} else if end < cur.start-1 {
			next := *cur
			*cur = Range{start: start, end: end, next: &next}
			break
		} else {
			oldE := cur.end
			cur.start = int(math.Min(float64(cur.start), float64(start)))
			cur.end = int(math.Max(float64(cur.end), float64(end)))
			if end > oldE && cur.next != nil {
				for next := cur.next; next != nil; next = next.next {
					if next.start <= end+1 {
						cur.end = next.end
						cur.next = next.next
						continue
					}
					break
				}
			}
			break
		}
	}
}

func NewBufferedPipe(bufSize int) *BufferedPipe {
	mu := &sync.Mutex{}
	return &BufferedPipe{
		bufSize:        int64(bufSize),
		writeBufOffset: -1,
		readBufOffset:  0,
		mu:             mu,
		readAvailable:  sync.NewCond(mu),
	}
}

type BufferedPipe struct {
	bufSize int64
	bufs    []*Buffer

	writeOffset    int64
	writeBufOffset int64

	readOffset    int64
	readBufOffset int64
	readCount     int64

	mu            *sync.Mutex
	readAvailable *sync.Cond
	eof           bool
	err           error
}

func (r *BufferedPipe) updatePos(rt Action, id int64) {
	if rt == writeAction {
		if !r.bufs[id].Filled() {
			return
		}

		for r.writeBufOffset < id {
			if !r.bufs[r.writeBufOffset+1].Filled() {
				break
			}
			r.writeBufOffset++ // Increment before broadcasting (instead of next loop iteration), or add locks aroung it.
			r.readAvailable.Broadcast()
		}
	} else if rt == readAction {
		for r.readBufOffset <= id {
			if !r.bufs[r.readBufOffset].Emptied() {
				break
			}
			r.bufs[r.readBufOffset] = nil // TODO: Could be worth using sync.Pool to reuse objects
			r.readBufOffset++
		}
	}
}

func (r *BufferedPipe) Close() error {
	r.eof = true
	r.err = io.EOF
	// Blindly assume all buffers up to the last have been filled, and eventually last one is not complete
	r.writeBufOffset = int64(len(r.bufs) - 1)
	r.readAvailable.Broadcast() // Ensure we unblock all readers
	return nil
}

func (r *BufferedPipe) CloseWithError(err error) error {
	if err == nil {
		r.err = io.EOF
	} else {
		r.err = err
	}
	r.Close()
	return nil
}

func (r *BufferedPipe) WriteAt(p []byte, off int64) (int, error) {
	var written int
	for written < len(p) {
		id := (off + int64(written)) / r.bufSize
		if id < r.writeBufOffset {
			return written, errors.New("not allowed to write to previous buffer position")
		}

		if len(r.bufs) <= int(id) {
			bb := make([]*Buffer, id+1)
			copy(bb, r.bufs)
			r.bufs = bb
		}
		buf := r.bufs[id]
		if buf == nil {
			buf = &Buffer{b: make([]byte, r.bufSize)}
			r.bufs[id] = buf
		}
		buf.mu.Lock()

		offset := int((off + int64(written)) % r.bufSize)
		w := copy(buf.b[offset:], p[written:])
		buf.UpdateRange(writeAction, offset, offset+w-1)
		buf.mu.Unlock()

		r.updatePos(writeAction, id)
		written += w
	}
	return written, nil
}

func (r *BufferedPipe) ReadAt(p []byte, off int64) (int, error) {
	var read int
	for read < len(p) {
		id := (off + int64(read)) / r.bufSize

		// We need to wait  for our buffer to be fully written
		r.mu.Lock()
		for id > r.writeBufOffset && !r.eof {
			r.readAvailable.Wait()
		}
		r.mu.Unlock()
		if id > r.writeBufOffset && len(r.bufs) <= int(id) {
			if read > 0 {
				return read, nil
			}
			return read, io.EOF
		}

		offset := int((off + int64(read)) % r.bufSize)
		end := r.bufs[id].written.end + 1 // +1 because end of slice is excluded
		if r.eof && (id > r.writeBufOffset || offset == end) {
			if read > 0 {
				return read, nil
			}
			return read, r.err
		}
		rr := copy(p[read:], r.bufs[id].b[offset:end])
		r.readCount += int64(rr)
		read += rr
		r.bufs[id].UpdateRange(readAction, offset, offset+rr-1)
		r.updatePos(readAction, id)
	}
	return read, nil
}

func (r *BufferedPipe) Read(p []byte) (n int, err error) {
	n, err = r.ReadAt(p, r.readOffset)
	r.readOffset += int64(n)
	return
}

func (r *BufferedPipe) Write(p []byte) (n int, err error) {
	n, err = r.WriteAt(p, r.writeOffset)
	r.writeOffset += int64(n)
	return
}
