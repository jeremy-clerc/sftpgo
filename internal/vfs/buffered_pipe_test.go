package vfs

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferedPipeSequentialWriteRead(t *testing.T) {
	p := NewBufferedPipe(10)
	data := bytes.Repeat([]byte("A"), 25)

	n, err := p.WriteAt(data, 0)
	require.NoError(t, err)
	assert.Equal(t, 25, n)

	p.Close()

	out := make([]byte, 25)
	total := 0
	for total < 25 {
		n, err := p.Read(out[total:])
		total += n
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
	}
	assert.Equal(t, 25, total)
	assert.Equal(t, data, out)
}

func TestBufferedPipeConcurrentWriteRead(t *testing.T) {
	p := NewBufferedPipe(1024)
	data := bytes.Repeat([]byte("Hello, World!\n"), 10000)

	var wg sync.WaitGroup
	wg.Add(2)

	var readData []byte
	var readErr error

	go func() {
		defer wg.Done()
		// Write sequentially using WriteAt
		chunkSize := 32768
		for off := 0; off < len(data); off += chunkSize {
			end := off + chunkSize
			if end > len(data) {
				end = len(data)
			}
			_, err := p.WriteAt(data[off:end], int64(off))
			if err != nil {
				t.Errorf("write error: %v", err)
				return
			}
		}
		p.Close()
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := p.Read(buf)
			if n > 0 {
				readData = append(readData, buf[:n]...)
			}
			if err != nil {
				if err != io.EOF {
					readErr = err
				}
				break
			}
		}
	}()

	wg.Wait()
	require.NoError(t, readErr)
	assert.Equal(t, data, readData)
}

func TestBufferedPipeOutOfOrderWriteAt(t *testing.T) {
	// Buffer size of 10, write chunks out of order within the same buffer window
	p := NewBufferedPipe(10)

	// Write second chunk first (offset 10-19), then first chunk (0-9)
	_, err := p.WriteAt([]byte("BBBBBBBBBB"), 10)
	require.NoError(t, err)

	_, err = p.WriteAt([]byte("AAAAAAAAAA"), 0)
	require.NoError(t, err)

	// Write third chunk
	_, err = p.WriteAt([]byte("CCCCC"), 20)
	require.NoError(t, err)

	p.Close()

	out := make([]byte, 25)
	total := 0
	for total < 25 {
		n, err := p.Read(out[total:])
		total += n
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
	}
	assert.Equal(t, 25, total)
	expected := append(append(bytes.Repeat([]byte("A"), 10), bytes.Repeat([]byte("B"), 10)...), bytes.Repeat([]byte("C"), 5)...)
	assert.Equal(t, expected, out)
}

func TestBufferedPipeCloseWithError(t *testing.T) {
	p := NewBufferedPipe(10)

	testErr := io.ErrUnexpectedEOF
	p.CloseWithError(testErr)

	buf := make([]byte, 10)
	_, err := p.Read(buf)
	assert.ErrorIs(t, err, testErr)
}

func TestBufferedPipeWriteAtMultiplePacketsAhead(t *testing.T) {
	// Simulates an SFTP client sending multiple packets ahead
	p := NewBufferedPipe(32768)
	total := 128 * 1024 // 128KB
	data := bytes.Repeat([]byte("X"), total)

	var wg sync.WaitGroup
	wg.Add(2)

	var readData []byte

	go func() {
		defer wg.Done()
		// Write in 32KB chunks, sometimes slightly out of order
		chunkSize := 32768
		offsets := make([]int, 0)
		for off := 0; off < total; off += chunkSize {
			offsets = append(offsets, off)
		}
		// Simulate out-of-order: swap pairs
		for i := 0; i+1 < len(offsets); i += 2 {
			offsets[i], offsets[i+1] = offsets[i+1], offsets[i]
		}
		for _, off := range offsets {
			end := off + chunkSize
			if end > total {
				end = total
			}
			_, err := p.WriteAt(data[off:end], int64(off))
			if err != nil {
				t.Errorf("write error at offset %d: %v", off, err)
				return
			}
		}
		p.Close()
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := p.Read(buf)
			if n > 0 {
				readData = append(readData, buf[:n]...)
			}
			if err != nil {
				break
			}
		}
	}()

	wg.Wait()
	assert.Equal(t, data, readData)
}
