package vfs

import (
	"io"
	"sync"
)

const (
	defaultBufferedPipeBufSize = 256 * 1024      // 256KB per buffer slot
	defaultMaxBufferSize       = 16 * 1024 * 1024 // 16MB max total buffer memory
)

// BufferedPipe is an in-memory pipe that supports WriteAt for out-of-order
// writes within a bounded window, while providing sequential Read for consumers
// like S3 multipart upload. Memory usage is bounded: only a sliding window of
// buffers is kept alive; once a buffer is fully written and read, it is freed.
type BufferedPipe struct {
	bufSize  int64
	maxSlots int64
	bufs     []*pipeBuf

	// writePos tracks the highest contiguous buffer index that is fully written.
	// Reader can read up to and including this buffer.
	writePos int64
	// readPos tracks the lowest buffer index not yet fully read.
	readPos int64
	// readOffset is the sequential read cursor.
	readOffset int64

	mu            *sync.Mutex
	readAvailable *sync.Cond
	slotFreed     *sync.Cond

	closed bool
	err    error
}

type pipeBuf struct {
	mu      sync.Mutex
	data    []byte
	written int // number of bytes written into this buffer
	read    int // number of bytes read from this buffer
	full    bool
}

// NewBufferedPipe creates a new BufferedPipe with the given per-buffer size.
// If bufSize <= 0, defaultBufferedPipeBufSize is used.
func NewBufferedPipe(bufSize int) *BufferedPipe {
	if bufSize <= 0 {
		bufSize = defaultBufferedPipeBufSize
	}
	maxSlots := int64(defaultMaxBufferSize) / int64(bufSize)
	if maxSlots < 2 {
		maxSlots = 2
	}
	mu := &sync.Mutex{}
	return &BufferedPipe{
		bufSize:       int64(bufSize),
		maxSlots:      maxSlots,
		writePos:      -1,
		mu:            mu,
		readAvailable: sync.NewCond(mu),
		slotFreed:     sync.NewCond(mu),
	}
}

// getBuf returns the buffer for the given slot index, allocating it if needed.
// Must be called with p.mu held.
func (p *BufferedPipe) getBuf(id int64) *pipeBuf {
	idx := int(id)
	if idx < len(p.bufs) && p.bufs[idx] != nil {
		return p.bufs[idx]
	}
	// Grow the slice if needed.
	if idx >= len(p.bufs) {
		newBufs := make([]*pipeBuf, idx+1)
		copy(newBufs, p.bufs)
		p.bufs = newBufs
	}
	if p.bufs[idx] == nil {
		p.bufs[idx] = &pipeBuf{data: make([]byte, p.bufSize)}
	}
	return p.bufs[idx]
}

// WriteAt writes p at the given offset. Writes can arrive slightly out of order
// (e.g. SFTP clients sending multiple packets ahead) as long as they target
// buffers within a reasonable window ahead of the read cursor.
func (p *BufferedPipe) WriteAt(data []byte, off int64) (int, error) {
	written := 0
	for written < len(data) {
		id := (off + int64(written)) / p.bufSize
		offset := int((off + int64(written)) % p.bufSize)

		// Backpressure: block if the writer is too far ahead of the reader.
		// Also get or allocate the buffer under the same lock to avoid races.
		p.mu.Lock()
		for id-p.readPos >= p.maxSlots && !p.closed {
			p.slotFreed.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return written, p.err
		}
		buf := p.getBuf(id)
		p.mu.Unlock()

		buf.mu.Lock()
		w := copy(buf.data[offset:], data[written:])
		buf.written += w
		if buf.written >= int(p.bufSize) {
			buf.full = true
		}
		buf.mu.Unlock()

		// Advance writePos if consecutive buffers are full.
		p.mu.Lock()
		for p.writePos+1 < int64(len(p.bufs)) {
			next := p.bufs[int(p.writePos+1)]
			if next == nil || !next.full {
				break
			}
			p.writePos++
			p.readAvailable.Broadcast()
		}
		p.mu.Unlock()

		written += w
	}
	return written, nil
}

// Read reads sequentially from the pipe. It blocks until data is available or
// the pipe is closed. It returns available data as soon as possible rather than
// waiting to fill the entire buffer, to avoid stalling the pipeline.
func (p *BufferedPipe) Read(data []byte) (int, error) {
	read := 0
	for read < len(data) {
		id := p.readOffset / p.bufSize
		offset := int(p.readOffset % p.bufSize)

		// Wait for this buffer to be available for reading.
		p.mu.Lock()
		if id > p.writePos && !p.closed {
			// If we already have data, return it instead of blocking.
			if read > 0 {
				p.mu.Unlock()
				return read, nil
			}
			for id > p.writePos && !p.closed {
				p.readAvailable.Wait()
			}
		}
		closed := p.closed
		writePos := p.writePos
		pipeErr := p.err
		var buf *pipeBuf
		if int(id) < len(p.bufs) {
			buf = p.bufs[int(id)]
		}
		p.mu.Unlock()

		if id > writePos {
			// Pipe is closed, check if there's a partial last buffer.
			if buf == nil {
				if read > 0 {
					return read, nil
				}
				return 0, pipeErr
			}
			buf.mu.Lock()
			avail := buf.written
			if offset >= avail {
				buf.mu.Unlock()
				if read > 0 {
					return read, nil
				}
				return 0, pipeErr
			}
			n := copy(data[read:], buf.data[offset:avail])
			buf.read += n
			buf.mu.Unlock()
			read += n
			p.readOffset += int64(n)
			return read, nil
		}

		buf.mu.Lock()
		end := int(p.bufSize)
		if !buf.full {
			end = buf.written
		}
		n := copy(data[read:], buf.data[offset:end])
		buf.read += n
		fullyRead := buf.read >= int(p.bufSize)
		buf.mu.Unlock()

		read += n
		p.readOffset += int64(n)

		// Free buffer once fully read.
		if fullyRead {
			p.mu.Lock()
			p.bufs[int(id)] = nil
			if p.readPos <= id {
				p.readPos = id + 1
			}
			p.slotFreed.Broadcast()
			p.mu.Unlock()
		}

		// If pipe is closed and we've read everything available, return.
		if closed && id >= writePos && offset+n >= end {
			if read > 0 {
				return read, nil
			}
			return 0, pipeErr
		}
	}
	return read, nil
}

// Close marks the pipe as closed. The reader will drain remaining data and
// then receive io.EOF.
func (p *BufferedPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	if p.err == nil {
		p.err = io.EOF
	}
	// Mark the last buffer's writePos so reader can finish.
	if len(p.bufs) > 0 {
		p.writePos = int64(len(p.bufs) - 1)
	}
	p.readAvailable.Broadcast()
	p.slotFreed.Broadcast()
	return nil
}

// CloseWithError closes the pipe with the given error. If err is nil, io.EOF is used.
func (p *BufferedPipe) CloseWithError(err error) error {
	p.mu.Lock()
	if err != nil {
		p.err = err
	} else {
		p.err = io.EOF
	}
	p.mu.Unlock()
	return p.Close()
}
