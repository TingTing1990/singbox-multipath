package multipath

import (
	"errors"
	"github.com/sagernet/sing/common/pipe"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type logicalConn struct {
	readConn                          net.Conn
	writeConn                         net.Conn
	onClose                           func() error
	onCloseRead                       func() error
	closeOne                          sync.Once
	writeCloseOne                     sync.Once
	writeClosed                       chan struct{}
	writeDeadline                     pipe.Deadline
	errorMu                           sync.RWMutex
	readError, writeError             error
	readClosedByApp, writeClosedByApp atomic.Bool
}

func newLogicalPipe() (*logicalConn, net.Conn, net.Conn) {
	appWrite, coreRead := net.Pipe()
	coreWrite, appRead := net.Pipe()
	return &logicalConn{readConn: appRead, writeConn: appWrite, writeClosed: make(chan struct{}), writeDeadline: pipe.MakeDeadline()}, coreRead, coreWrite
}

func (c *logicalConn) Read(buffer []byte) (int, error) {
	n, err := c.readConn.Read(buffer)
	if err != nil {
		if c.readClosedByApp.Load() {
			return n, net.ErrClosed
		}
		c.errorMu.RLock()
		if c.readError != nil {
			err = c.readError
		}
		c.errorMu.RUnlock()
	}
	return n, err
}

func (c *logicalConn) Write(buffer []byte) (int, error) {
	n, err := c.writeConn.Write(buffer)
	if err != nil {
		if c.writeClosedByApp.Load() {
			return n, net.ErrClosed
		}
		c.errorMu.RLock()
		if c.writeError != nil {
			err = c.writeError
		}
		c.errorMu.RUnlock()
	}
	return n, err
}

func (c *logicalConn) setTerminalError(err error, drainReceive bool) {
	readErr, writeErr := err, err
	if errors.Is(err, io.EOF) {
		readErr, writeErr = io.ErrUnexpectedEOF, net.ErrClosed
	}
	c.errorMu.Lock()
	if !drainReceive {
		c.readError = readErr
	}
	c.writeError = writeErr
	c.errorMu.Unlock()
}

func (c *logicalConn) CloseRead() error {
	c.readClosedByApp.Store(true)
	if c.onCloseRead != nil {
		return c.onCloseRead()
	}
	return c.readConn.Close()
}

func (c *logicalConn) CloseWrite() error {
	c.writeClosedByApp.Store(true)
	return c.closeWriteInternal()
}

func (c *logicalConn) closeWriteInternal() error {
	c.writeCloseOne.Do(func() { close(c.writeClosed); c.writeDeadline.Set(time.Time{}) })
	return c.writeConn.Close()
}

func (c *logicalConn) closeInternal() (error, bool) {
	var closeErr error
	closed := false
	c.closeOne.Do(func() {
		closed = true
		closeErr = errors.Join(c.readConn.Close(), c.closeWriteInternal())
	})
	return closeErr, closed
}

func (c *logicalConn) Close() error {
	c.readClosedByApp.Store(true)
	c.writeClosedByApp.Store(true)
	if c.onClose != nil {
		return c.onClose()
	}
	closeErr, _ := c.closeInternal()
	return closeErr
}

func (c *logicalConn) LocalAddr() net.Addr {
	return c.readConn.LocalAddr()
}

func (c *logicalConn) RemoteAddr() net.Addr {
	return c.readConn.RemoteAddr()
}

func (c *logicalConn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.SetReadDeadline(deadline), c.SetWriteDeadline(deadline))
}

func (c *logicalConn) SetReadDeadline(deadline time.Time) error {
	return c.readConn.SetReadDeadline(deadline)
}

func (c *logicalConn) SetWriteDeadline(deadline time.Time) error {
	if err := c.writeConn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	c.writeDeadline.Set(deadline)
	return nil
}
