package vfs

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func FuzzBufferedPipe(f *testing.F) {
	f.Add(bytes.Repeat([]byte("This is a wonderful world.\n"), 1000))
	f.Fuzz(func(t *testing.T, data []byte) {
		written := make([]byte, 0, len(data))
		p := NewBufferedPipe(1024)
		wDone := make(chan struct{})
		rDone := make(chan struct{})
		go func() {
			t.Log("will write data")
			i, err := p.Write(data)
			if err != nil {
				t.Errorf("failed to write data: %v", err)
			}
			p.Close()
			close(wDone)
			t.Logf("data written (%v)", i)
		}()
		go func() {
			t.Log("will read data")
			readBuf := make([]byte, 1024)
			for {
				i, err := p.Read(readBuf)
				written = append(written, readBuf[:i]...)
				t.Logf("read data (%v), %v", len(written), err)
				if err != nil {
					if err != io.EOF {
						t.Errorf("failed read: %v", err)
					}
					break
				}
			}
			close(rDone)
		}()
		<-wDone
		<-rDone
		if !reflect.DeepEqual(data, written) {
			t.Errorf("data: %v; written: %v", data, written)
		}
	})
}

func TestBufferedPipe(t *testing.T) {
	r := NewBufferedPipe(10) // Buffer of 10 bytes, though we can have any number of buffer as we write.
	r.Write(bytes.Repeat([]byte("1"), 25))
	if len(r.bufs) != 3 {
		t.Fatalf("buf size 10, wrote 25 bytes, expected 3 buffers; got %v buffer(s)", len(r.bufs))
	}
	rBuf := make([]byte, 20)
	if n, err := r.Read(rBuf); n != 20 || err != nil {
		t.Fatalf("failed to read 20 bytes, read %v, err: %v", n, err)
	}
	if r.bufs[0] != nil || r.bufs[1] != nil {
		t.Error("read buffers have not been cleaned up")
	}
	r.Close()
	if n, err := r.Read(rBuf); n != 5 || err != nil {
		t.Fatalf("failed to read last 5 bytes, read %v, err: %v", n, err)
	}
}
