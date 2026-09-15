package multipath

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/pipe"
)

const (
	frameTypeData          byte = 1
	frameTypeFIN           byte = 3
	frameTypeReset         byte = 4
	frameTypeSessionClose  byte = 5
	frameTypeSenderStatus  byte = 6
	frameTypePing          byte = 7
	frameTypePong          byte = 8
	frameTypeWindow        byte = 9
	frameTypeWindowRequest byte = 10

	dataFrameHeaderSize      = 13 // type(1) + seq(8) + len(4)
	controlFrameHeaderSize   = 9  // type(1) + seq(8)
	maxFramePayload          = 1 << 20
	maxQueueBytes            = 64 << 20
	maxReorderBytes          = 512 << 20
	maxReplayBytes           = 512 << 20
	sessionCloseDrainTimeout = time.Second // Logical shutdown does not wait for this drain.
)

var (
	errCoreClosed  = errors.New("multipath core closed")
	errLeg1Stalled = errors.New("multipath booster leg stalled")
)

type coreConfig struct {
	AggregationEnabled             bool
	ActivationOnQueue              bool
	ChunkSize                      int
	QueueFrames                    int
	QueueBytes                     int64
	ThresholdBytesPS               uint64
	ActivationAfterBytes           uint64
	ActivationAfterBytesMinBytesPS uint64
	ActivationWindow               time.Duration
	BandwidthMbps                  []uint32
	MaxReorderFrames               int
	MaxReorderBytes                int64
	ReplayBytes                    int64
	ReplayTimeout                  time.Duration
	Memory                         *memoryBudget
	OnLeg1Active                   func(activationInfo, bool)
	OnLegFailure                   func(uint8, legFailureStage, error)
	OnStatusEvent                  func()
	OnProtocolError                func(error)
	SendStatus                     bool
}

type legFailureStage string

const (
	legFailureWriteControl legFailureStage = "write_control"
	legFailureWriteData    legFailureStage = "write_data"
	legFailureHandshake    legFailureStage = "handshake_response"
	legFailureReadData     legFailureStage = "read_data"
	legFailureReplay       legFailureStage = "replay_timeout"
)

type activationReason string

const (
	activationReasonBytes      activationReason = "bytes"
	activationReasonThroughput activationReason = "throughput"
	activationReasonLeg0Queue  activationReason = "leg0_queue"
)

type activationInfo struct {
	Reason           activationReason
	CurrentBytes     uint64
	ThresholdBytes   uint64
	WindowBytes      uint64
	RateBytesPS      uint64
	ThresholdBytesPS uint64
	MinRateBytesPS   uint64
	Elapsed          time.Duration
	BacklogBytes     int64
	QueueBytes       int64
	RequiredDuration time.Duration
}

func (i activationInfo) String() string {
	switch i.Reason {
	case activationReasonBytes:
		if i.MinRateBytesPS > 0 {
			return fmt.Sprintf(
				"reason=%s current_bytes=%d threshold_bytes=%d measured_mbps=%.2f min_mbps=%.2f window=%s",
				i.Reason,
				i.CurrentBytes,
				i.ThresholdBytes,
				float64(i.RateBytesPS)*8/1_000_000,
				float64(i.MinRateBytesPS)*8/1_000_000,
				i.Elapsed.Round(time.Millisecond),
			)
		}
		return fmt.Sprintf("reason=%s current_bytes=%d threshold_bytes=%d", i.Reason, i.CurrentBytes, i.ThresholdBytes)
	case activationReasonThroughput:
		return fmt.Sprintf(
			"reason=%s measured_mbps=%.2f threshold_mbps=%.2f window=%s window_bytes=%d",
			i.Reason,
			float64(i.RateBytesPS)*8/1_000_000,
			float64(i.ThresholdBytesPS)*8/1_000_000,
			i.Elapsed.Round(time.Millisecond),
			i.WindowBytes,
		)
	case activationReasonLeg0Queue:
		ratio := float64(0)
		if i.QueueBytes > 0 {
			ratio = float64(i.BacklogBytes) * 100 / float64(i.QueueBytes)
		}
		return fmt.Sprintf(
			"reason=%s backlog_bytes=%d queue_bytes=%d ratio=%.1f%% duration=%s required_duration=%s",
			i.Reason,
			i.BacklogBytes,
			i.QueueBytes,
			ratio,
			i.Elapsed.Round(time.Millisecond),
			i.RequiredDuration,
		)
	default:
		return fmt.Sprintf("reason=%s", i.Reason)
	}
}

type wireFrame struct {
	typ         byte
	seq         uint64
	data        []byte
	replay      bool
	status      senderStatus
	flow        flowMessage
	reordered   bool
	primaryOnly bool
}

type replayEntry struct {
	frame          wireFrame
	startedAt      time.Time
	fallbackQueued bool
}

type ownedBuffer struct {
	data           []byte
	refs           int
	receive        bool
	primaryReserve bool
}

type mpLegCounters struct {
	txBytes  atomic.Uint64
	rxBytes  atomic.Uint64
	txFrames atomic.Uint64
	rxFrames atomic.Uint64
}

type legShutdownRequest struct {
	err       error
	frameType byte
	status    *senderStatus
}

// logicalConn uses one net.Pipe per direction. Unlike a single net.Pipe, this
// lets sing-box half-close one direction without tearing down queued data in
// the other direction.
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

// Record the transport error before closing either pipe, so a blocked Read
// cannot mistake an aborted stream for a clean FIN. Complete RX may still drain.
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

type mpLeg struct {
	id                    uint8
	ctx                   context.Context
	cancel                context.CancelFunc
	conn                  net.Conn
	readPreamble          func(net.Conn) error
	send                  chan wireFrame
	recovery              chan wireFrame
	queueMu               sync.Mutex
	flowWake              chan struct{}
	flowMu                sync.Mutex
	flowFrames            [2]wireFrame
	flowPending           [2]bool
	ready                 atomic.Bool
	control               chan wireFrame
	telemetry             chan struct{}
	telemetryMu           sync.Mutex
	telemetryFrame        wireFrame
	telemetryPending      bool
	shutdown              chan legShutdownRequest
	onClose               func(error)
	done                  chan struct{}
	writerDone            chan struct{}
	readerDone            chan struct{}
	closeOne              sync.Once
	queuedBytes           atomic.Int64
	writingBytes          atomic.Int64
	writeStarted          atomic.Int64
	transportWriteStarted atomic.Int64
}

func (l *mpLeg) Done() <-chan struct{} {
	return l.done
}

func (l *mpLeg) close(err error) {
	l.closeOne.Do(func() {
		l.queueMu.Lock()
		close(l.done)
		l.queueMu.Unlock()
		l.cancel()
		_ = l.conn.Close()
		if l.onClose != nil {
			l.onClose(err)
		}
	})
}

func (l *mpLeg) requestShutdown(err error, frameType byte, status *senderStatus) {
	select {
	case <-l.done:
	case l.shutdown <- legShutdownRequest{err: err, frameType: frameType, status: status}:
	default:
		l.close(err)
	}
}

func (l *mpLeg) backlogBytes() int64 {
	return l.queuedBytes.Load() + l.writingBytes.Load()
}

func (l *mpLeg) writingSnapshot(now time.Time) (int64, time.Duration) {
	writing := l.writingBytes.Load()
	started := l.writeStarted.Load()
	if writing <= 0 || started <= 0 {
		return writing, 0
	}
	return writing, max(0, now.Sub(time.Unix(0, started)))
}

func (l *mpLeg) reserveQueue(length int64, limit int64) bool {
	for {
		queued := l.queuedBytes.Load()
		if queued+length > limit {
			return false
		}
		if l.queuedBytes.CompareAndSwap(queued, queued+length) {
			return true
		}
	}
}

func (l *mpLeg) tryQueue(frame wireFrame, limit int64) bool {
	l.queueMu.Lock()
	defer l.queueMu.Unlock()
	select {
	case <-l.done:
		return false
	default:
	}
	length := int64(len(frame.data))
	if !l.reserveQueue(length, limit) {
		return false
	}
	select {
	case <-l.done:
		l.queuedBytes.Add(-length)
		return false
	case l.send <- frame:
		return true
	default:
		l.queuedBytes.Add(-length)
		return false
	}
}

func (l *mpLeg) queueControl(coreDone <-chan struct{}, frame wireFrame) error {
	select {
	case <-coreDone:
		return errCoreClosed
	case <-l.done:
		return errCoreClosed
	case l.control <- frame:
		return nil
	}
}

func (l *mpLeg) tryQueueControl(frame wireFrame) bool {
	select {
	case <-l.done:
		return false
	case l.control <- frame:
		return true
	default:
		return false
	}
}

func (l *mpLeg) queueLatestTelemetry(frame wireFrame) bool {
	select {
	case <-l.done:
		return false
	default:
	}
	l.telemetryMu.Lock()
	l.telemetryFrame = frame
	l.telemetryPending = true
	l.telemetryMu.Unlock()
	select {
	case l.telemetry <- struct{}{}:
	default:
	}
	return true
}

func (l *mpLeg) takeTelemetry() (wireFrame, bool) {
	l.telemetryMu.Lock()
	frame := l.telemetryFrame
	pending := l.telemetryPending
	l.telemetryPending = false
	l.telemetryMu.Unlock()
	return frame, pending
}

type mpCore struct {
	cfg             coreConfig
	ctx             context.Context
	cancel          context.CancelFunc
	appConn         *logicalConn
	txPipe          net.Conn
	rxPipe          net.Conn
	legsMu          sync.RWMutex
	legs            map[uint8]*mpLeg
	reserved        map[uint8]bool
	retiring        map[uint8]*mpLeg
	done            chan struct{}
	released        chan struct{}
	closeOne        sync.Once
	txSeq           atomic.Uint64
	ingressBytes    atomic.Uint64
	egressBytes     atomic.Uint64
	legCounters     [2]mpLegCounters
	active          atomic.Bool
	activeCh        chan struct{}
	activateOnce    sync.Once
	activationMu    sync.Mutex
	activation      activationInfo
	activationAt    time.Time
	notifiedLeg1    *mpLeg
	leg1Joins       uint64
	localFIN        atomic.Bool
	remoteFIN       atomic.Bool
	receivedFIN     atomic.Bool
	ackedFIN        atomic.Bool
	localClosing    atomic.Bool
	localReadClosed atomic.Bool
	ackedNext       atomic.Uint64
	rxExpected      atomic.Uint64
	replayMu        sync.Mutex
	replay          map[uint64]*replayEntry
	replayBytes     int64
	reorderBytes    atomic.Int64
	reorderCount    atomic.Int64
	replayPeak      atomic.Int64
	reorderPeak     atomic.Int64
	reorderFPeak    atomic.Int64
	legPeak         [2]atomic.Int64
	fallbackB       atomic.Uint64
	fallbackF       atomic.Uint64
	fallbackE       atomic.Uint64
	replayTO        atomic.Uint64
	backpressE      atomic.Uint64
	backpressNS     atomic.Uint64
	legFailureMu    sync.Mutex
	legFailures     [2]uint64
	lastFailLeg     uint8
	lastFailStage   legFailureStage
	failureMu       sync.Mutex
	failure         string
	failureAt       time.Time
	memory          *memoryBudget
	sessionBytes    int64
	bufferMu        sync.Mutex
	buffers         map[*byte]*ownedBuffer
	flow            *flowControl
	txReserve       chan []byte
	peerStatusMu    sync.Mutex
	peerStatus      peerSenderStatus
	statusMu        sync.Mutex
	statusSeq       uint64
	lastStatus      senderStatus
	lastStatusAt    time.Time
	probeMu         sync.Mutex
	probeNext       uint64
	probePending    [2]pendingProbe
	probeLast       [2]time.Time
	probeRTT        [2]legRTTSnapshot
	workerMu        sync.Mutex
	workerGroup     sync.WaitGroup
	workerClosed    bool
}

func newCore(parent context.Context, cfg coreConfig) (*mpCore, net.Conn) {
	core, appConn, err := newCoreWithError(parent, cfg)
	if err != nil {
		panic(err)
	}
	return core, appConn
}

func newCoreWithError(parent context.Context, cfg coreConfig) (*mpCore, net.Conn, error) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 64 * 1024
	}
	if cfg.QueueFrames <= 0 {
		cfg.QueueFrames = 256
	}
	if cfg.QueueBytes <= 0 {
		cfg.QueueBytes = int64(cfg.ChunkSize) * int64(cfg.QueueFrames)
	}
	if cfg.ActivationWindow <= 0 {
		cfg.ActivationWindow = time.Second
	}
	if cfg.MaxReorderFrames <= 0 {
		cfg.MaxReorderFrames = 2048
	}
	if cfg.MaxReorderBytes <= 0 {
		cfg.MaxReorderBytes = 64 << 20
	}
	if cfg.ReplayBytes <= 0 {
		cfg.ReplayBytes = 64 << 20
	}
	if cfg.ReplayTimeout <= 0 {
		cfg.ReplayTimeout = time.Second
	}
	if parent == nil {
		parent = context.Background()
	}
	memory := cfg.Memory
	if memory == nil {
		memory = newMemoryBudget(1<<62, false)
	}
	sessionBytes := sessionMemoryReservation(cfg)
	initialCredit := int64(cfg.ChunkSize) + receiveFrameOverhead
	if !memory.reserveSession(sessionBytes + initialCredit) {
		return nil, nil, errMemoryLimit
	}
	memory.sessions.Add(1)
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	appConn, txPipe, rxPipe := newLogicalPipe()
	c := &mpCore{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		appConn:      appConn,
		txPipe:       txPipe,
		rxPipe:       rxPipe,
		legs:         make(map[uint8]*mpLeg),
		reserved:     make(map[uint8]bool),
		retiring:     make(map[uint8]*mpLeg),
		done:         make(chan struct{}),
		released:     make(chan struct{}),
		activeCh:     make(chan struct{}),
		replay:       make(map[uint64]*replayEntry),
		memory:       memory,
		sessionBytes: sessionBytes,
		buffers:      make(map[*byte]*ownedBuffer),
		txReserve:    make(chan []byte, 1),
	}
	c.flow = newFlowControl(cfg)
	// A bounded memory-backed startup grant prevents the first response from
	// waiting a window RTT after a single chunk. Larger windows are on demand.
	startup := min(c.flow.rxCapacity, memory.receiveShareSlots(c.receiveSlotBytes()), uint64(max(1, min(c.cfg.QueueFrames*2, (32<<20)/cfg.ChunkSize))))
	c.flow.rxLimit += memory.reserveStartup(startup-1, c.receiveSlotBytes())
	c.flow.rxWanted = c.flow.rxLimit
	c.txReserve <- memory.takeReservedBuffer(cfg.ChunkSize)
	appConn.onClose = c.closeApplication
	appConn.onCloseRead = func() error {
		c.localReadClosed.Store(true)
		return appConn.readConn.Close()
	}
	c.startWorkers(c.txLoop, c.rxLoop, c.activationLoop, c.flowLoop, c.replayLoop)
	return c, appConn, nil
}

func (c *mpCore) startWorkers(workers ...func()) bool {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()
	if c.workerClosed {
		return false
	}
	for _, worker := range workers {
		c.workerGroup.Add(1)
		go func(run func()) {
			defer c.workerGroup.Done()
			run()
		}(worker)
	}
	return true
}

func (c *mpCore) stopWorkerAdmission() {
	c.workerMu.Lock()
	c.workerClosed = true
	c.workerMu.Unlock()
}

func (c *mpCore) Context() context.Context {
	return c.ctx
}

func (c *mpCore) AppConn() net.Conn {
	return c.appConn
}

func (c *mpCore) Done() <-chan struct{} {
	return c.done
}

func (c *mpCore) Close() error {
	c.fail(net.ErrClosed)
	// Service shutdown and explicit core disposal must also interrupt a peer
	// close that is waiting for the local application to drain buffered data.
	_, _ = c.appConn.closeInternal()
	return nil
}

// Application Close follows TCP semantics: reject further local I/O, but let
// already accepted TX drain in the background. Only a receipt ACK covering FIN
// permits session-close to overtake neither queued data nor delayed booster data.
func (c *mpCore) closeApplication() error {
	c.localClosing.Store(true)
	c.localReadClosed.Store(true)
	err, _ := c.appConn.closeInternal()
	if c.getLeg(0) == nil {
		c.fail(io.EOF)
	} else {
		c.finishApplicationClose()
	}
	return err
}

func (c *mpCore) finishApplicationClose() {
	if c.localClosing.Load() && c.ackedFIN.Load() {
		c.terminate(io.EOF, frameTypeSessionClose)
	}
}

func (c *mpCore) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *mpCore) fail(err error) {
	c.terminate(err, frameTypeSessionClose)
}

func (c *mpCore) peerSessionClosed(err error) {
	c.terminateWithReceiveDrain(err, 0, errors.Is(err, io.EOF) && c.receiveComplete())
}

func (c *mpCore) terminate(err error, terminalFrameType byte) {
	c.terminateWithReceiveDrain(err, terminalFrameType, false)
}

func (c *mpCore) terminateWithReceiveDrain(err error, terminalFrameType byte, drainReceive bool) {
	if err == nil {
		err = errCoreClosed
	}
	c.closeOne.Do(func() {
		c.stopWorkerAdmission()
		c.appConn.setTerminalError(err, drainReceive)
		c.failureMu.Lock()
		c.failure = err.Error()
		c.failureAt = time.Now()
		c.failureMu.Unlock()
		if terminalFrameType == frameTypeReset && c.cfg.OnProtocolError != nil {
			c.cfg.OnProtocolError(err)
		}
		var finalStatus *senderStatus
		if c.cfg.SendStatus {
			status := c.nextSenderStatus(time.Now())
			finalStatus = &status
		}
		c.legsMu.RLock()
		legs := make([]*mpLeg, 0, len(c.legs))
		for _, leg := range c.legs {
			legs = append(legs, leg)
		}
		c.legsMu.RUnlock()
		for _, leg := range legs {
			var legStatus *senderStatus
			if leg.id == 0 {
				legStatus = finalStatus
			}
			leg.requestShutdown(err, terminalFrameType, legStatus)
		}
		c.cancel()
		_ = c.txPipe.Close()
		if drainReceive {
			// Receipt ACKs promise ownership, not application consumption. Keep
			// the read half alive until rxLoop delivers all bytes preceding FIN.
			_ = c.appConn.closeWriteInternal()
		} else {
			_ = c.rxPipe.Close()
			_, _ = c.appConn.closeInternal()
		}
		// Done promises that subsequent application writes are rejected.
		// Publish it only after closing TX; complete RX may continue draining.
		close(c.done)
		c.replayMu.Lock()
		c.replay = make(map[uint64]*replayEntry)
		c.replayBytes = 0
		c.replayMu.Unlock()
		go c.releaseAfterShutdown(legs, err)
	})
}

func (c *mpCore) releaseAfterShutdown(legs []*mpLeg, err error) {
	closeLegsAfterDrain(legs, err)
	c.workerGroup.Wait()
	c.releaseAllBuffers()
	select {
	case buffer := <-c.txReserve:
		c.memory.putReservedBuffer(buffer)
	default:
	}
	c.releaseReceiveWindow()
	c.memory.releaseSession(c.sessionBytes)
	c.memory.sessions.Add(-1)
	close(c.released)
}

func (c *mpCore) protocolFail(err error) {
	c.terminate(err, frameTypeReset)
}

func closeLegsAfterDrain(legs []*mpLeg, err error) {
	timer := time.NewTimer(sessionCloseDrainTimeout)
	defer timer.Stop()
	for _, leg := range legs {
		select {
		case <-leg.writerDone:
		case <-timer.C:
			for _, pendingLeg := range legs {
				pendingLeg.close(err)
			}
			return
		}
	}
	for _, leg := range legs {
		leg.close(err)
	}
}

func (c *mpCore) reserveLeg(id uint8) error {
	if id > 1 {
		return errors.New("invalid multipath leg id")
	}
	c.legsMu.Lock()
	defer c.legsMu.Unlock()
	if c.isDone() {
		return errCoreClosed
	}
	if previous := c.retiring[id]; previous != nil {
		select {
		case <-previous.readerDone:
		default:
			return errors.New("previous multipath reader is draining")
		}
		select {
		case <-previous.writerDone:
		default:
			return errors.New("previous multipath writer is draining")
		}
		delete(c.retiring, id)
	}
	if c.legs[id] != nil || c.reserved[id] {
		return errors.New("duplicate multipath leg")
	}
	c.reserved[id] = true
	return nil
}

func (c *mpCore) cancelLegReservation(id uint8) {
	c.legsMu.Lock()
	delete(c.reserved, id)
	c.legsMu.Unlock()
}

func (c *mpCore) commitLeg(id uint8, conn net.Conn, onClose func(error)) (*mpLeg, error) {
	return c.commitLegWithReadPreamble(id, conn, onClose, nil)
}

func (c *mpCore) commitLegWithReadPreamble(id uint8, conn net.Conn, onClose func(error), readPreamble func(net.Conn) error) (*mpLeg, error) {
	c.legsMu.Lock()
	if !c.reserved[id] {
		c.legsMu.Unlock()
		return nil, errors.New("multipath leg is not reserved")
	}
	delete(c.reserved, id)
	if c.isDone() {
		c.legsMu.Unlock()
		return nil, errCoreClosed
	}
	if c.legs[id] != nil {
		c.legsMu.Unlock()
		return nil, errors.New("duplicate multipath leg")
	}
	legCtx, legCancel := context.WithCancel(c.ctx)
	leg := &mpLeg{
		id:           id,
		ctx:          legCtx,
		cancel:       legCancel,
		conn:         conn,
		readPreamble: readPreamble,
		send:         make(chan wireFrame, c.cfg.QueueFrames),
		recovery:     make(chan wireFrame, c.cfg.QueueFrames),
		flowWake:     make(chan struct{}, 1),
		control:      make(chan wireFrame, 32),
		telemetry:    make(chan struct{}, 1),
		shutdown:     make(chan legShutdownRequest, 1),
		onClose:      onClose,
		done:         make(chan struct{}),
		writerDone:   make(chan struct{}),
		readerDone:   make(chan struct{}),
	}
	leg.ready.Store(readPreamble == nil)
	if id == 0 {
		c.flow.rxMu.Lock()
		initial := wireFrame{typ: frameTypeWindow, flow: flowMessage{Limit: c.flow.rxLimit}}
		if early, ok := conn.(*clientFastOpenConn); ok {
			// Keep hello, startup credit and first DATA in one physical write;
			// do not start a lazy child just to advertise credit.
			encoded := encodeFlow(initial)
			early.initialWindow = encoded[:]
		} else if readPreamble == nil {
			leg.queueFlow(initial)
		}
		c.flow.rxMu.Unlock()
	}
	c.legs[id] = leg
	if !c.startWorkers(func() { c.legWriteLoop(leg) }, func() { c.legReadLoop(leg) }) {
		delete(c.legs, id)
		c.legsMu.Unlock()
		legCancel()
		return nil, errCoreClosed
	}
	c.legsMu.Unlock()
	if id == 1 {
		c.flow.txMu.Lock()
		c.flow.stallSince = time.Time{}
		c.flow.peerDeliveryRTT = 0
		c.flow.txMu.Unlock()
		c.notifyLeg1Active()
	}
	return leg, nil
}

func (c *mpCore) addLeg(id uint8, conn net.Conn, onClose func(error)) (*mpLeg, error) {
	return c.addLegWithReadPreamble(id, conn, onClose, nil)
}

func (c *mpCore) addLegWithReadPreamble(id uint8, conn net.Conn, onClose func(error), readPreamble func(net.Conn) error) (*mpLeg, error) {
	if err := c.reserveLeg(id); err != nil {
		return nil, err
	}
	leg, err := c.commitLegWithReadPreamble(id, conn, onClose, readPreamble)
	if err != nil {
		c.cancelLegReservation(id)
	}
	return leg, err
}

func (c *mpCore) getLeg(id uint8) *mpLeg {
	c.legsMu.RLock()
	leg := c.legs[id]
	c.legsMu.RUnlock()
	return leg
}

func (c *mpCore) availableLegs() []*mpLeg {
	c.legsMu.RLock()
	legs := make([]*mpLeg, 0, 2)
	if leg := c.legs[0]; leg != nil {
		legs = append(legs, leg)
	}
	if leg := c.legs[1]; leg != nil {
		legs = append(legs, leg)
	}
	c.legsMu.RUnlock()
	return legs
}

func (c *mpCore) getBuffer(ctx context.Context, class memoryClass) ([]byte, error) {
	buffer, err := c.memory.acquire(ctx, c.cfg.ChunkSize, class)
	if err != nil {
		return nil, err
	}
	key := &buffer[0]
	c.bufferMu.Lock()
	c.buffers[key] = &ownedBuffer{data: buffer, refs: 1}
	c.bufferMu.Unlock()
	return buffer, nil
}

func (c *mpCore) putBuffer(buffer []byte) {
	if cap(buffer) != c.cfg.ChunkSize {
		return
	}
	fullBuffer := buffer[:cap(buffer)]
	key := &fullBuffer[0]
	c.bufferMu.Lock()
	owned, loaded := c.buffers[key]
	if loaded {
		owned.refs--
		if owned.refs == 0 {
			delete(c.buffers, key)
		} else {
			loaded = false
		}
	}
	c.bufferMu.Unlock()
	if loaded {
		if owned.primaryReserve {
			c.txReserve <- owned.data
		} else if owned.receive {
			c.memory.putReservedBuffer(owned.data)
		} else {
			c.memory.release(owned.data)
		}
	}
}

func (c *mpCore) retainBuffer(buffer []byte) {
	c.bufferMu.Lock()
	if owned := c.buffers[&buffer[:cap(buffer)][0]]; owned != nil {
		owned.refs++
	}
	c.bufferMu.Unlock()
}

func (c *mpCore) releaseAllBuffers() {
	c.bufferMu.Lock()
	buffers := make([]*ownedBuffer, 0, len(c.buffers))
	for _, buffer := range c.buffers {
		buffers = append(buffers, buffer)
	}
	c.buffers = make(map[*byte]*ownedBuffer)
	c.bufferMu.Unlock()
	for _, buffer := range buffers {
		if buffer.receive || buffer.primaryReserve {
			c.memory.putReservedBuffer(buffer.data)
		} else {
			c.memory.release(buffer.data)
		}
	}
}

func (c *mpCore) txLoop() {
	for {
		buffer, primaryOnly, bufferErr := c.getTXBuffer()
		if bufferErr != nil {
			return
		}
		n, err := c.txPipe.Read(buffer[:c.cfg.ChunkSize])
		if n > 0 {
			c.ingressBytes.Add(uint64(n))
			sequence, sequenceErr := c.reserveTXSequence()
			if sequenceErr != nil {
				c.putBuffer(buffer)
				if !c.isDone() {
					c.protocolFail(sequenceErr)
				}
				return
			}
			frame := wireFrame{
				typ:         frameTypeData,
				seq:         sequence,
				data:        buffer[:n],
				primaryOnly: primaryOnly,
			}
			if enqueueErr := c.enqueue(frame); enqueueErr != nil {
				c.putBuffer(buffer)
				if !c.isDone() {
					c.fail(enqueueErr)
				}
				return
			}
		} else {
			c.putBuffer(buffer)
		}
		if err != nil {
			if c.isDone() {
				return
			}
			if errors.Is(err, io.EOF) {
				if finErr := c.sendFIN(); finErr != nil && !c.isDone() {
					c.fail(finErr)
				}
				return
			}
			c.protocolFail(err)
			return
		}
	}
}

func (c *mpCore) sendFIN() error {
	if !c.localFIN.CompareAndSwap(false, true) {
		return nil
	}
	leg := c.getLeg(0)
	if leg == nil {
		return errors.New("multipath control leg is unavailable")
	}
	return leg.queueControl(c.done, wireFrame{typ: frameTypeFIN, seq: c.txSeq.Load()})
}

// An omitted rate keeps the original default; explicit zero always disables it.
func resolveActivationThreshold(threshold *uint32, afterBytes uint64) uint64 {
	if threshold != nil {
		return uint64(*threshold) * 1_000_000 / 8
	}
	if afterBytes == 0 {
		return 150 * 1_000_000 / 8
	}
	return 0
}

func (c *mpCore) activationLoop() {
	if !c.cfg.AggregationEnabled || c.active.Load() ||
		(!c.cfg.ActivationOnQueue && c.cfg.ThresholdBytesPS == 0 && c.cfg.ActivationAfterBytes == 0) {
		return
	}
	interval := c.cfg.ActivationWindow / 10
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	windowStart := time.Now()
	windowBase := c.ingressBytes.Load()
	var queueHighSince time.Time
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			if c.active.Load() {
				return
			}
			bytesNow := c.ingressBytes.Load()
			if info, ok := activationAfterBytes(c.cfg, bytesNow, windowBase, now.Sub(windowStart)); ok {
				c.activate(info)
				return
			}
			if (c.cfg.ThresholdBytesPS > 0 || (c.cfg.ActivationAfterBytes > 0 && c.cfg.ActivationAfterBytesMinBytesPS > 0)) && now.Sub(windowStart) >= c.cfg.ActivationWindow {
				delta := bytesNow - windowBase
				elapsed := now.Sub(windowStart)
				rate := uint64(0)
				if elapsed > 0 {
					rate = uint64(float64(delta) / elapsed.Seconds())
				}
				if c.cfg.ThresholdBytesPS > 0 && rate >= c.cfg.ThresholdBytesPS {
					c.activate(activationInfo{
						Reason:           activationReasonThroughput,
						WindowBytes:      delta,
						RateBytesPS:      rate,
						ThresholdBytesPS: c.cfg.ThresholdBytesPS,
						Elapsed:          elapsed,
					})
					return
				}
				windowStart = now
				windowBase = bytesNow
			}
			if !c.cfg.ActivationOnQueue {
				continue
			}
			primary := c.getLeg(0)
			if primary == nil {
				continue
			}
			backlogBytes := primary.backlogBytes()
			if backlogBytes*5 >= c.cfg.QueueBytes*4 {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow {
					c.activate(activationInfo{
						Reason:           activationReasonLeg0Queue,
						BacklogBytes:     backlogBytes,
						QueueBytes:       c.cfg.QueueBytes,
						Elapsed:          now.Sub(queueHighSince),
						RequiredDuration: c.cfg.ActivationWindow,
					})
					return
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

func activationAfterBytes(cfg coreConfig, bytesNow, windowBase uint64, elapsed time.Duration) (activationInfo, bool) {
	if cfg.ActivationAfterBytes == 0 || bytesNow < cfg.ActivationAfterBytes {
		return activationInfo{}, false
	}
	info := activationInfo{
		Reason:         activationReasonBytes,
		CurrentBytes:   bytesNow,
		ThresholdBytes: cfg.ActivationAfterBytes,
	}
	if cfg.ActivationAfterBytesMinBytesPS == 0 {
		return info, true
	}
	if elapsed < cfg.ActivationWindow || elapsed <= 0 {
		return activationInfo{}, false
	}
	delta := bytesNow - windowBase
	rate := uint64(float64(delta) / elapsed.Seconds())
	if rate < cfg.ActivationAfterBytesMinBytesPS {
		return activationInfo{}, false
	}
	info.RateBytesPS = rate
	info.MinRateBytesPS = cfg.ActivationAfterBytesMinBytesPS
	info.Elapsed = elapsed
	return info, true
}

func (c *mpCore) activate(info activationInfo) {
	if !c.cfg.AggregationEnabled {
		return
	}
	c.activateOnce.Do(func() {
		c.activationMu.Lock()
		c.activation = info
		c.activationAt = time.Now()
		c.activationMu.Unlock()
		c.active.Store(true)
		close(c.activeCh)
		c.notifyLeg1Active()
	})
}

func (c *mpCore) notifyLeg1Active() {
	if !c.active.Load() || c.cfg.OnLeg1Active == nil {
		return
	}
	leg := c.getLeg(1)
	if leg == nil {
		return
	}
	c.activationMu.Lock()
	if c.notifiedLeg1 == leg {
		c.activationMu.Unlock()
		return
	}
	info := c.activation
	reconnect := c.leg1Joins > 0
	c.notifiedLeg1 = leg
	c.leg1Joins++
	callback := c.cfg.OnLeg1Active
	c.activationMu.Unlock()
	callback(info, reconnect)
}

func (c *mpCore) weightFor(id uint8) uint32 {
	if int(id) < len(c.cfg.BandwidthMbps) && c.cfg.BandwidthMbps[id] > 0 {
		return c.cfg.BandwidthMbps[id]
	}
	return 1
}

func (c *mpCore) chooseLeg(frameLength int) *mpLeg {
	if !c.active.Load() || c.txSeq.Load() <= 1 {
		return c.getLeg(0)
	}
	c.flow.txMu.Lock()
	paused := c.flow.recovering
	c.flow.txMu.Unlock()
	legs := c.availableLegs()
	var best *mpLeg
	var bestNum, bestDen uint64
	for _, leg := range legs {
		if leg.id == 1 && (paused || !c.memory.boosterAllowed()) {
			continue
		}
		weight := uint64(c.weightFor(leg.id))
		num := uint64(leg.backlogBytes() + int64(frameLength))
		if best == nil || num*bestDen < bestNum*weight {
			best = leg
			bestNum = num
			bestDen = weight
		}
	}
	return best
}

func (c *mpCore) tryQueueNewFrame(leg *mpLeg, frame wireFrame) bool {
	if leg == nil {
		return false
	}
	queuedFrame := frame
	if leg.id == 1 {
		c.flow.txMu.Lock()
		defer c.flow.txMu.Unlock()
		paused := c.flow.recovering
		if frame.seq == 0 || frame.primaryOnly || paused || !c.memory.boosterAllowed() {
			return false
		}
		if !c.trackReplay(&queuedFrame) {
			return false
		}
	}
	if leg.tryQueue(queuedFrame, c.cfg.QueueBytes) {
		updateAtomicPeak(&c.legPeak[leg.id], leg.backlogBytes())
		return true
	}
	if queuedFrame.replay {
		c.untrackReplay(queuedFrame.seq)
	}
	return false
}

func (c *mpCore) enqueue(frame wireFrame) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var blockedAt time.Time
	markBlocked := func() {
		if blockedAt.IsZero() {
			blockedAt = time.Now()
			c.backpressE.Add(1)
		}
	}
	finishBlocked := func() {
		if !blockedAt.IsZero() {
			c.backpressNS.Add(uint64(time.Since(blockedAt)))
			blockedAt = time.Time{}
		}
	}
	for {
		select {
		case <-c.done:
			finishBlocked()
			return errCoreClosed
		default:
		}
		leg := c.chooseLeg(len(frame.data))
		if c.tryQueueNewFrame(leg, frame) {
			finishBlocked()
			return nil
		}
		if c.active.Load() {
			for _, other := range c.availableLegs() {
				if other != leg && c.tryQueueNewFrame(other, frame) {
					finishBlocked()
					return nil
				}
			}
		}
		markBlocked()
		select {
		case <-c.done:
			finishBlocked()
			return errCoreClosed
		case <-ticker.C:
		}
	}
}

func (c *mpCore) legWriteLoop(leg *mpLeg) {
	defer close(leg.writerDone)
	defer func() {
		for {
			select {
			case frame := <-leg.send:
				leg.queuedBytes.Add(-int64(len(frame.data)))
				c.putBuffer(frame.data)
			case frame := <-leg.recovery:
				leg.queuedBytes.Add(-int64(len(frame.data)))
				c.putBuffer(frame.data)
			default:
				return
			}
		}
	}()
	for {
		select {
		case request := <-leg.shutdown:
			c.finishLegShutdown(leg, request)
			return
		default:
		}
		select {
		case control := <-leg.control:
			if err := c.writeControlFrame(leg, control); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
			continue
		default:
		}
		if flow, pending := leg.takeFlow(); pending {
			if err := writeWireFrame(leg.conn, flow); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
			continue
		}
		select {
		case frame := <-leg.recovery:
			if !c.writeDataFrame(leg, frame) {
				return
			}
			continue
		default:
		}
		select {
		case request := <-leg.shutdown:
			c.finishLegShutdown(leg, request)
			return
		case <-c.done:
			select {
			case request := <-leg.shutdown:
				c.finishLegShutdown(leg, request)
			default:
				leg.close(errCoreClosed)
			}
			return
		case <-leg.done:
			return
		case control := <-leg.control:
			if err := c.writeControlFrame(leg, control); err != nil {
				c.legFailed(leg, legFailureWriteControl, err)
				return
			}
		case <-leg.flowWake:
		case <-leg.telemetry:
			if frame, pending := leg.takeTelemetry(); pending {
				if err := writeWireFrame(leg.conn, frame); err != nil {
					c.legFailed(leg, legFailureWriteControl, err)
					return
				}
			}
		case frame := <-leg.recovery:
			if !c.writeDataFrame(leg, frame) {
				return
			}
		case frame := <-leg.send:
			if !c.writeDataFrame(leg, frame) {
				return
			}
		}
	}
}

func (c *mpCore) writeDataFrame(leg *mpLeg, frame wireFrame) bool {
	length := int64(len(frame.data))
	leg.queuedBytes.Add(-length)
	defer c.putBuffer(frame.data) // Queue ownership is independent of replay ownership.
	if frame.replay && frame.seq < c.ackedNext.Load() {
		return true
	}
	if leg.id == 1 {
		c.replayMu.Lock()
		entry := c.replay[frame.seq]
		reassigned := entry == nil || entry.fallbackQueued
		if !reassigned {
			entry.startedAt = time.Now()
		}
		c.replayMu.Unlock()
		if reassigned {
			return true
		}
	}
	leg.writingBytes.Add(length)
	leg.writeStarted.Store(time.Now().UnixNano())
	leg.transportWriteStarted.Store(time.Now().UnixNano())
	err := writeWireFrame(leg.conn, frame)
	leg.transportWriteStarted.Store(0)
	leg.writeStarted.Store(0)
	leg.writingBytes.Add(-length)
	if err != nil {
		c.legFailed(leg, legFailureWriteData, err)
		return false
	}
	c.legCounters[leg.id].txBytes.Add(uint64(length))
	c.legCounters[leg.id].txFrames.Add(1)
	wakeFlow(c.flow.wake)
	return true
}

func (c *mpCore) writeControlFrame(leg *mpLeg, frame wireFrame) error {
	if frame.typ == frameTypePing {
		c.markProbeSent(leg.id, frame.seq, time.Now())
	}
	leg.transportWriteStarted.Store(time.Now().UnixNano())
	err := writeWireFrame(leg.conn, frame)
	leg.transportWriteStarted.Store(0)
	if err != nil && frame.typ == frameTypePing {
		c.cancelProbe(leg.id, frame.seq)
	}
	return err
}

func (c *mpCore) finishLegShutdown(leg *mpLeg, request legShutdownRequest) {
	if request.status != nil {
		_ = writeWireFrame(leg.conn, wireFrame{typ: frameTypeSenderStatus, status: *request.status})
	}
	if request.frameType != 0 {
		_ = writeWireFrame(leg.conn, wireFrame{typ: request.frameType})
	}
	leg.close(request.err)
}

func (c *mpCore) legReadLoop(leg *mpLeg) {
	defer close(leg.readerDone)
	if leg.readPreamble != nil {
		if err := leg.readPreamble(leg.conn); err != nil {
			if !c.isDone() {
				c.legFailed(leg, legFailureHandshake, err)
			}
			return
		}
	}
	leg.ready.Store(true)
	wakeFlow(c.flow.wake)
	for {
		frame, err := readWireFrameForLeg(leg.ctx, leg.conn, c, leg.id)
		if err != nil {
			if !c.isDone() {
				c.legFailed(leg, legFailureReadData, err)
			}
			return
		}
		switch frame.typ {
		case frameTypeWindow, frameTypeWindowRequest:
			if leg.id != 0 {
				c.protocolFail(errors.New("multipath flow control received on booster leg"))
				return
			}
			if frame.typ == frameTypeWindow {
				err = c.handleWindow(frame.flow)
			} else {
				err = c.handleWindowRequest(frame.flow)
			}
			if err != nil {
				c.protocolFail(err)
				return
			}
		case frameTypePing:
			leg.tryQueueControl(wireFrame{typ: frameTypePong, seq: frame.seq})
		case frameTypePong:
			c.handlePong(leg.id, frame.seq, time.Now())
		case frameTypeSenderStatus:
			if leg.id != 0 {
				c.protocolFail(errors.New("multipath sender status received on booster leg"))
				return
			}
			c.handlePeerSenderStatus(frame.status, time.Now())
		case frameTypeReset:
			c.peerSessionClosed(errors.New("multipath peer reset"))
			return
		case frameTypeSessionClose:
			c.peerSessionClosed(io.EOF)
			return
		case frameTypeData, frameTypeFIN:
			if frame.typ == frameTypeData {
				c.legCounters[leg.id].rxBytes.Add(uint64(len(frame.data)))
				c.legCounters[leg.id].rxFrames.Add(1)
			}
			if err = c.receiveFrame(frame, leg.id); err != nil {
				c.protocolFail(err)
				return
			}
		default:
			c.protocolFail(errors.New("unknown multipath frame type"))
			return
		}
	}
}

func (c *mpCore) legFailed(leg *mpLeg, stage legFailureStage, err error) {
	c.legsMu.Lock()
	if c.legs[leg.id] != leg {
		c.legsMu.Unlock()
		leg.close(err)
		return
	}
	delete(c.legs, leg.id)
	c.retiring[leg.id] = leg
	c.legsMu.Unlock()
	leg.close(err)
	c.cancelLegProbe(leg.id)
	c.legFailureMu.Lock()
	c.legFailures[leg.id]++
	c.lastFailLeg = leg.id
	c.lastFailStage = stage
	c.legFailureMu.Unlock()
	if c.cfg.OnStatusEvent != nil {
		c.cfg.OnStatusEvent()
	}
	if c.cfg.OnLegFailure != nil {
		c.cfg.OnLegFailure(leg.id, stage, err)
	}
	if leg.id == 0 {
		// A peer close can surface as a write error before its session-close
		// is read. Transport failure cannot invalidate already complete RX.
		if c.receiveComplete() {
			c.terminateWithReceiveDrain(err, 0, true)
		} else {
			c.fail(err)
		}
		return
	}
	c.startWorkers(c.reinjectLeg1)
}

func (c *mpCore) rxLoop() {
	f := c.flow
	defer func() { c.reorderBytes.Store(0); c.reorderCount.Store(0) }()
	defer c.rxPipe.Close()
	for {
		f.rxMu.Lock()
		frame, ready := f.rxPending[f.rxConsumed]
		if ready {
			delete(f.rxPending, f.rxConsumed)
		}
		finished := f.rxFIN != nil && f.rxConsumed == *f.rxFIN
		f.rxMu.Unlock()
		if finished {
			c.remoteFIN.Store(true)
			_ = c.rxPipe.Close()
			return
		}
		if !ready {
			select {
			case <-c.done:
				return
			case <-f.rxWake:
			}
			continue
		}
		if err := writeAll(c.rxPipe, frame.data); err != nil {
			c.putBuffer(frame.data)
			if c.localReadClosed.Load() && !c.isDone() {
				// A locally closed application abandons RX, not its buffered TX.
				// Continue consuming credit so the peer can process our final ACK.
				c.consumeFrame()
				continue
			}
			if !c.isDone() {
				c.fail(err)
			}
			return
		}
		c.egressBytes.Add(uint64(len(frame.data)))
		c.putBuffer(frame.data)
		c.consumeFrame()
	}
}

func (c *mpCore) trackReplay(frame *wireFrame) bool {
	length := int64(len(frame.data))
	c.replayMu.Lock()
	defer c.replayMu.Unlock()
	if c.replayBytes+length > c.cfg.ReplayBytes {
		return false
	}
	if _, exists := c.replay[frame.seq]; exists {
		return false
	}
	frame.replay = true
	c.retainBuffer(frame.data)
	c.replay[frame.seq] = &replayEntry{frame: *frame}
	c.replayBytes += length
	updateAtomicPeak(&c.replayPeak, c.replayBytes)
	return true
}

func (c *mpCore) untrackReplay(seq uint64) {
	var buffer []byte
	c.replayMu.Lock()
	if entry := c.replay[seq]; entry != nil {
		delete(c.replay, seq)
		c.replayBytes -= int64(len(entry.frame.data))
		buffer = entry.frame.data
	}
	c.replayMu.Unlock()
	if buffer != nil {
		c.putBuffer(buffer)
	}
}

func (c *mpCore) handleACK(next uint64) {
	if next > c.txSeq.Load() {
		c.protocolFail(errors.New("invalid multipath ACK sequence"))
		return
	}
	for {
		current := c.ackedNext.Load()
		if next <= current {
			return
		}
		if c.ackedNext.CompareAndSwap(current, next) {
			break
		}
	}
	var buffers [][]byte
	c.replayMu.Lock()
	for seq, entry := range c.replay {
		if seq < next {
			delete(c.replay, seq)
			c.replayBytes -= int64(len(entry.frame.data))
			buffers = append(buffers, entry.frame.data)
		}
	}
	c.replayMu.Unlock()
	for _, buffer := range buffers {
		c.putBuffer(buffer)
	}
}

func (c *mpCore) replayLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			timeout := c.recoveryTimeout()
			c.flow.txMu.Lock()
			c.replayMu.Lock()
			hasPending := len(c.replay) != 0
			// Queue residence, or ACK delay behind a leg0 gap, is not leg1 loss.
			entry := c.replay[c.ackedNext.Load()]
			eligible := entry != nil && !entry.fallbackQueued && entry.frame.seq >= c.flow.peerLeg1Next
			var started time.Time
			if entry != nil {
				started = entry.startedAt
			}
			c.replayMu.Unlock()
			leg1 := c.getLeg(1)
			if entry != nil && started.IsZero() && leg1 != nil {
				// A probe may be stuck ahead of the first DATA write. Queue age
				// alone is not a timeout, but a blocked transport write is.
				if blocked := leg1.transportWriteStarted.Load(); blocked != 0 {
					started = time.Unix(0, blocked)
				}
			}
			if !hasPending && (leg1 == nil || leg1.writingBytes.Load() == 0 && leg1.transportWriteStarted.Load() == 0) {
				// No outstanding data and no retained partial write: an idle leg
				// is not stalled merely because no new delivery counter arrives.
				c.flow.stallSince = time.Time{}
				c.flow.recovering = false
			}
			if !started.IsZero() && c.flow.peerProgressAt.After(started) {
				started = c.flow.peerProgressAt
			}
			stalled := eligible && !started.IsZero() && now.Sub(started) >= timeout
			beginRecovery := stalled && !c.flow.recovering
			if beginRecovery {
				c.flow.recovering = true
				c.flow.stallSince = now
			}
			recovering := c.flow.recovering
			persistent := recovering && !c.flow.stallSince.IsZero() && now.Sub(c.flow.stallSince) >= max(2*time.Second, 5*timeout)
			c.flow.txMu.Unlock()
			if persistent {
				if leg := c.getLeg(1); leg != nil {
					c.legFailed(leg, legFailureReplay, errLeg1Stalled)
				}
			}
			if beginRecovery {
				c.replayTO.Add(1)
				if c.cfg.OnStatusEvent != nil {
					c.cfg.OnStatusEvent()
				}
			}
			if recovering && !persistent {
				c.recoverBatch()
			}
		}
	}
}

func (c *mpCore) recoveryTimeout() time.Duration {
	timeout := c.cfg.ReplayTimeout
	c.flow.txMu.Lock()
	// Receipt timing also includes data already buffered by the child. Small
	// probes alone underestimate this during a rapidly growing send backlog.
	timeout = max(timeout, 2*c.flow.peerDeliveryRTT)
	c.flow.txMu.Unlock()
	stats := c.rttSnapshot()
	if stats[1].Samples > 0 {
		estimate := max(200*time.Millisecond, stats[1].EWMA+4*stats[1].Jitter)
		if stats[0].Samples > 0 {
			estimate = max(estimate, stats[0].EWMA+4*stats[0].Jitter)
		}
		timeout = max(timeout, estimate)
	}
	return max(100*time.Millisecond, timeout)
}

// Recover a bounded prefix without waiting for queue space. A real leg failure
// still uses reinjectLeg1, but a delayed ACK must not move the entire backlog.
func (c *mpCore) recoverBatch() {
	c.replayMu.Lock()
	sequences := make([]uint64, 0, len(c.replay))
	for seq, entry := range c.replay {
		if !entry.fallbackQueued {
			sequences = append(sequences, seq)
		}
	}
	c.replayMu.Unlock()
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	limit := max(1, min(16, (1<<20)/c.cfg.ChunkSize))
	queued := 0
	for _, seq := range sequences {
		if queued >= limit {
			break
		}
		leg := c.getLeg(0)
		if leg == nil {
			return
		}
		c.replayMu.Lock()
		entry := c.replay[seq]
		if entry == nil || entry.fallbackQueued {
			c.replayMu.Unlock()
			continue
		}
		frame := entry.frame
		c.retainBuffer(frame.data)
		entry.fallbackQueued = true
		leg.queueMu.Lock()
		ok := false
		select {
		case <-leg.done:
		default:
			leg.queuedBytes.Add(int64(len(frame.data)))
			select {
			case leg.recovery <- frame:
				ok = true
			default:
				leg.queuedBytes.Add(-int64(len(frame.data)))
			}
		}
		leg.queueMu.Unlock()
		if !ok {
			entry.fallbackQueued = false
		}
		c.replayMu.Unlock()
		if !ok {
			c.putBuffer(frame.data)
			break
		}
		queued++
		c.fallbackB.Add(uint64(len(frame.data)))
		c.fallbackF.Add(1)
		updateAtomicPeak(&c.legPeak[0], leg.backlogBytes())
	}
	if queued > 0 {
		c.fallbackE.Add(1)
	}
}

func (c *mpCore) reinjectLeg1() {
	c.replayMu.Lock()
	sequences := make([]uint64, 0, len(c.replay))
	for seq, entry := range c.replay {
		if !entry.fallbackQueued {
			entry.fallbackQueued = true
			sequences = append(sequences, seq)
		}
	}
	c.replayMu.Unlock()
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	if len(sequences) == 0 {
		return
	}
	c.fallbackE.Add(1)
	for _, seq := range sequences {
		c.replayMu.Lock()
		entry := c.replay[seq]
		if entry == nil {
			c.replayMu.Unlock()
			continue
		}
		frame := entry.frame
		c.retainBuffer(frame.data)
		c.replayMu.Unlock()
		leg0 := c.getLeg(0)
		if leg0 == nil {
			c.putBuffer(frame.data)
			if !c.isDone() {
				c.fail(errors.New("multipath control leg unavailable during fallback"))
			}
			return
		}
		if !c.queueRecovery(leg0, frame) {
			c.putBuffer(frame.data)
			return
		}
		c.fallbackB.Add(uint64(len(frame.data)))
		c.fallbackF.Add(1)
	}
}

func (c *mpCore) queueRecovery(leg *mpLeg, frame wireFrame) bool {
	for {
		leg.queueMu.Lock()
		select {
		case <-leg.done:
			leg.queueMu.Unlock()
			return false
		default:
		}
		length := int64(len(frame.data))
		leg.queuedBytes.Add(length)
		select {
		case leg.recovery <- frame:
			leg.queueMu.Unlock()
			updateAtomicPeak(&c.legPeak[0], leg.backlogBytes())
			return true
		default:
			leg.queuedBytes.Add(-length)
			leg.queueMu.Unlock()
		}
		select {
		case <-c.done:
			return false
		case <-leg.done:
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

func updateAtomicPeak(peak *atomic.Int64, value int64) {
	for current := peak.Load(); value > current; current = peak.Load() {
		if peak.CompareAndSwap(current, value) {
			return
		}
	}
}

func writeWireFrame(conn net.Conn, frame wireFrame) error {
	if writer, isInitialWriter := conn.(initialFrameWriter); isInitialWriter {
		handled, err := writer.writeInitialFrame(frame)
		if handled {
			return err
		}
	}
	switch frame.typ {
	case frameTypeWindow, frameTypeWindowRequest:
		data := encodeFlow(frame)
		return writeAll(conn, data[:])
	case frameTypeData:
		if len(frame.data) == 0 || len(frame.data) > maxFramePayload {
			return errors.New("invalid multipath data frame")
		}
		var header [dataFrameHeaderSize]byte
		header[0] = frameTypeData
		binary.BigEndian.PutUint64(header[1:9], frame.seq)
		binary.BigEndian.PutUint32(header[9:13], uint32(len(frame.data)))
		buffers := net.Buffers{header[:], frame.data}
		_, err := buffers.WriteTo(conn)
		return err
	case frameTypeFIN, frameTypePing, frameTypePong:
		var header [controlFrameHeaderSize]byte
		header[0] = frame.typ
		binary.BigEndian.PutUint64(header[1:9], frame.seq)
		return writeAll(conn, header[:])
	case frameTypeSenderStatus:
		return writeSenderStatus(conn, frame.status)
	case frameTypeReset, frameTypeSessionClose:
		return writeAll(conn, []byte{frame.typ})
	default:
		return errors.New("unknown multipath frame type")
	}
}

func readWireFrame(conn net.Conn, core *mpCore) (wireFrame, error) {
	return readWireFrameForLeg(core.ctx, conn, core, 0)
}

func readWireFrameForLeg(ctx context.Context, conn net.Conn, core *mpCore, legID uint8) (wireFrame, error) {
	var frame wireFrame
	var frameType [1]byte
	if _, err := io.ReadFull(conn, frameType[:]); err != nil {
		return frame, err
	}
	frame.typ = frameType[0]
	switch frame.typ {
	case frameTypeWindow, frameTypeWindowRequest:
		message, err := readFlow(conn)
		frame.flow = message
		return frame, err
	case frameTypeData:
		var header [dataFrameHeaderSize - 1]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return wireFrame{}, err
		}
		frame.seq = binary.BigEndian.Uint64(header[0:8])
		length := int(binary.BigEndian.Uint32(header[8:12]))
		if length <= 0 || length > core.cfg.ChunkSize || length > maxFramePayload {
			return wireFrame{}, errors.New("invalid multipath frame length")
		}
		buffer := core.memory.takeReservedBuffer(core.cfg.ChunkSize)
		buffer = buffer[:length]
		if _, err := io.ReadFull(conn, buffer); err != nil {
			core.memory.putReservedBuffer(buffer)
			return wireFrame{}, err
		}
		core.bufferMu.Lock()
		core.buffers[&buffer[0]] = &ownedBuffer{data: buffer[:cap(buffer)], refs: 1, receive: true}
		core.bufferMu.Unlock()
		frame.data = buffer
		return frame, nil
	case frameTypeFIN, frameTypePing, frameTypePong:
		var sequence [8]byte
		if _, err := io.ReadFull(conn, sequence[:]); err != nil {
			return wireFrame{}, err
		}
		frame.seq = binary.BigEndian.Uint64(sequence[:])
		return frame, nil
	case frameTypeSenderStatus:
		status, err := readSenderStatus(conn)
		if err != nil {
			return wireFrame{}, err
		}
		frame.status = status
		return frame, nil
	case frameTypeReset, frameTypeSessionClose:
		return frame, nil
	default:
		return wireFrame{}, errors.New("unknown multipath frame type")
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
