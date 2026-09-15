package multipath

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
	"unsafe"
)

const (
	receiveFrameOverhead int64 = int64(unsafe.Sizeof(wireFrame{}))*2 + 128
	flowPayloadSize            = 41
	flowFlagGap          byte  = 1
	flowFlagPressure     byte  = 2
	flowFlagFINAck       byte  = 4
)

// Credit is in full negotiated chunks, not payload lengths. Thus even a stream
// of one-byte frames cannot exceed the receiver's allocation or metadata budget.
// Epoch changes return unused credit only after the sender has stopped using it.
type flowMessage struct {
	Epoch, Next, Limit, GapAge, Leg1Bytes uint64
	Flags                                 byte
}

func encodeFlow(frame wireFrame) [1 + flowPayloadSize]byte {
	var data [1 + flowPayloadSize]byte
	data[0], data[1] = frame.typ, frame.flow.Flags
	for i, value := range []uint64{frame.flow.Epoch, frame.flow.Next, frame.flow.Limit, frame.flow.GapAge, frame.flow.Leg1Bytes} {
		binary.BigEndian.PutUint64(data[2+i*8:10+i*8], value)
	}
	return data
}

func readFlow(conn net.Conn) (flowMessage, error) {
	var data [flowPayloadSize]byte
	if _, err := io.ReadFull(conn, data[:]); err != nil {
		return flowMessage{}, err
	}
	if data[0] & ^byte(flowFlagGap|flowFlagPressure|flowFlagFINAck) != 0 {
		return flowMessage{}, errors.New("invalid multipath flow flags")
	}
	message := flowMessage{Flags: data[0]}
	for i, value := range []*uint64{&message.Epoch, &message.Next, &message.Limit, &message.GapAge, &message.Leg1Bytes} {
		*value = binary.BigEndian.Uint64(data[1+i*8 : 9+i*8])
	}
	return message, nil
}

type flowControl struct {
	rxMu                                               sync.Mutex
	rxEpoch, rxLimit, rxWanted, rxConsumed, rxReceived uint64
	rxLeg1Bytes                                        uint64
	rxCapacity                                         uint64
	rxPending                                          map[uint64]wireFrame
	rxFIN                                              *uint64
	rxGapSince                                         time.Time
	rxDirty                                            bool
	rxWake                                             chan struct{}

	txMu                       sync.Mutex
	txEpoch, txLimit, txWanted uint64
	txLast                     time.Time
	txWaiting, txDirty         bool
	txWake                     chan struct{}
	peerGap                    flowMessage
	peerLeg1Bytes              uint64
	stallSince, pauseUntil     time.Time
	wake                       chan struct{}
}

func newFlowControl(cfg coreConfig) *flowControl {
	return &flowControl{
		rxLimit: 1, rxWanted: 1,
		rxCapacity: max(1, min(uint64(cfg.MaxReorderFrames), uint64(cfg.MaxReorderBytes/int64(cfg.ChunkSize)))),
		rxPending:  make(map[uint64]wireFrame), rxWake: make(chan struct{}, 1),
		txLimit: 1, txWanted: 1, txWake: make(chan struct{}, 1), wake: make(chan struct{}, 1),
	}
}

func wakeFlow(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (c *mpCore) getTXBuffer() ([]byte, bool, error) {
	for {
		select {
		case <-c.done:
			return nil, false, errCoreClosed
		default:
		}
		buffer, changed := c.memory.tryAcquirePrimary(c.cfg.ChunkSize)
		if buffer != nil {
			c.bufferMu.Lock()
			c.buffers[&buffer[0]] = &ownedBuffer{data: buffer, refs: 1}
			c.bufferMu.Unlock()
			return buffer, false, nil
		}
		select {
		case <-c.done:
			return nil, false, errCoreClosed
		case buffer = <-c.txReserve:
			c.bufferMu.Lock()
			c.buffers[&buffer[0]] = &ownedBuffer{data: buffer, refs: 1, primaryReserve: true}
			c.bufferMu.Unlock()
			return buffer, true, nil
		case <-changed:
		}
	}
}

func (c *mpCore) reserveTXSequence() (uint64, error) {
	f := c.flow
	for {
		f.txMu.Lock()
		next := c.txSeq.Load()
		goal := uint64(min(4096, max(8, c.cfg.QueueFrames*4)))
		if next > ^uint64(0)-goal {
			f.txMu.Unlock()
			return 0, errors.New("multipath sequence space exhausted")
		}
		if next+goal > f.txWanted {
			f.txWanted = next + goal
			f.txDirty = true
			wakeFlow(f.wake)
		}
		if next < f.txLimit {
			c.txSeq.Add(1)
			f.txLast = time.Now()
			f.txWaiting = false
			f.txMu.Unlock()
			return next, nil
		}
		f.txWaiting = true
		f.txMu.Unlock()
		select {
		case <-c.done:
			return 0, errCoreClosed
		case <-f.txWake:
		}
	}
}

func (c *mpCore) handleWindowRequest(message flowMessage) error {
	f := c.flow
	f.rxMu.Lock()
	defer f.rxMu.Unlock()
	if message.Flags != 0 || message.Limit < message.Next || message.Next > f.rxLimit {
		return errors.New("invalid multipath window request")
	}
	if message.Epoch < f.rxEpoch {
		return nil
	}
	if message.Epoch > f.rxEpoch {
		// The sender retains one immediately usable slot, so idle -> active
		// does not require an extra window round trip for the first frame.
		end := max(message.Next+1, f.rxConsumed+1)
		if end > f.rxLimit || end < f.rxReceived {
			return errors.New("invalid multipath credit return")
		}
		for seq := range f.rxPending {
			if seq >= end {
				return errors.New("multipath returned credit is already in use")
			}
		}
		returned := f.rxLimit - end
		f.rxEpoch, f.rxLimit, f.rxWanted = message.Epoch, end, message.Limit
		c.memory.releaseSession(int64(returned) * c.receiveSlotBytes())
	} else {
		f.rxWanted = max(f.rxWanted, message.Limit)
	}
	f.rxDirty = true
	wakeFlow(f.wake)
	return nil
}

func (c *mpCore) handleWindow(message flowMessage) error {
	f := c.flow
	if message.Next > c.txSeq.Load() || message.Next > message.Limit {
		return errors.New("invalid multipath receive window")
	}
	if message.Flags&flowFlagFINAck != 0 && (!c.localFIN.Load() || message.Next != c.txSeq.Load()) {
		return errors.New("invalid multipath FIN acknowledgement")
	}
	f.txMu.Lock()
	if message.Epoch != f.txEpoch {
		f.txMu.Unlock()
		return nil
	}
	f.txLimit = max(f.txLimit, message.Limit)
	if message.Leg1Bytes > f.peerLeg1Bytes {
		f.peerLeg1Bytes = message.Leg1Bytes
		f.stallSince = time.Time{}
	}
	f.peerGap = message
	f.txMu.Unlock()
	c.handleACK(message.Next)
	if message.Flags&flowFlagFINAck != 0 {
		c.ackedFIN.Store(true)
		c.finishApplicationClose()
	}
	wakeFlow(f.txWake)
	return nil
}

func (c *mpCore) receiveSlotBytes() int64 {
	return int64(c.cfg.ChunkSize) + receiveFrameOverhead
}

func (c *mpCore) receiveComplete() bool {
	f := c.flow
	f.rxMu.Lock()
	complete := f.rxFIN != nil && f.rxReceived == *f.rxFIN
	f.rxMu.Unlock()
	return complete
}

func (c *mpCore) receiveFrame(frame wireFrame, legID uint8) error {
	f := c.flow
	f.rxMu.Lock()
	if legID == 1 && frame.typ == frameTypeData {
		f.rxLeg1Bytes += uint64(len(frame.data))
	}
	if frame.typ == frameTypeFIN {
		defer f.rxMu.Unlock()
		if (f.rxFIN != nil && *f.rxFIN != frame.seq) || frame.seq < f.rxReceived || frame.seq > f.rxLimit {
			return errors.New("invalid multipath FIN sequence")
		}
		for seq := range f.rxPending {
			if seq >= frame.seq {
				return errors.New("multipath buffered data after FIN")
			}
		}
		seq := frame.seq
		f.rxFIN = &seq
		f.rxDirty = true
		c.receivedFIN.Store(true)
		wakeFlow(f.rxWake)
		wakeFlow(f.wake)
		return nil
	}
	if frame.seq < f.rxReceived {
		f.rxDirty = true
		f.rxMu.Unlock()
		c.putBuffer(frame.data)
		wakeFlow(f.wake)
		return nil
	}
	if _, exists := f.rxPending[frame.seq]; exists {
		f.rxMu.Unlock()
		c.putBuffer(frame.data)
		return nil
	}
	if frame.seq >= f.rxLimit || (f.rxFIN != nil && frame.seq >= *f.rxFIN) {
		f.rxMu.Unlock()
		c.putBuffer(frame.data)
		return errors.New("multipath data outside granted receive window")
	}
	frame.reordered = frame.seq != f.rxReceived
	f.rxPending[frame.seq] = frame
	if frame.reordered {
		updateAtomicPeak(&c.reorderPeak, c.reorderBytes.Add(int64(len(frame.data))))
		updateAtomicPeak(&c.reorderFPeak, c.reorderCount.Add(1))
	}
	previous := f.rxReceived
	for {
		ready, ok := f.rxPending[f.rxReceived]
		if !ok {
			break
		}
		if ready.reordered {
			c.reorderBytes.Add(-int64(len(ready.data)))
			c.reorderCount.Add(-1)
			ready.reordered = false
			f.rxPending[f.rxReceived] = ready
		}
		f.rxReceived++
	}
	c.rxExpected.Store(f.rxReceived)
	if c.reorderCount.Load() > 0 {
		if f.rxGapSince.IsZero() || f.rxReceived != previous {
			f.rxGapSince = time.Now()
		}
	} else {
		f.rxGapSince = time.Time{}
	}
	f.rxDirty = true
	f.rxMu.Unlock()
	wakeFlow(f.rxWake)
	wakeFlow(f.wake)
	return nil
}

func (c *mpCore) consumeFrame() {
	f := c.flow
	f.rxMu.Lock()
	f.rxConsumed++
	if f.rxLimit == f.rxConsumed {
		// Recycle the last slot rather than releasing and re-acquiring it.
		// Every admitted session can always receive its next leg0 frame.
		f.rxLimit++
	} else {
		c.memory.releaseSession(c.receiveSlotBytes())
	}
	f.rxDirty = true
	f.rxMu.Unlock()
	wakeFlow(f.wake)
}

func (c *mpCore) releaseReceiveWindow() {
	f := c.flow
	f.rxMu.Lock()
	remaining := f.rxLimit - f.rxConsumed
	f.rxPending = nil
	f.rxMu.Unlock()
	c.memory.releaseSession(int64(remaining) * c.receiveSlotBytes())
}

func (l *mpLeg) queueFlow(frame wireFrame) {
	index := 0
	if frame.typ == frameTypeWindowRequest {
		index = 1
	}
	l.flowMu.Lock()
	l.flowFrames[index], l.flowPending[index] = frame, true
	l.flowMu.Unlock()
	wakeFlow(l.flowWake)
}

func (l *mpLeg) takeFlow() (wireFrame, bool) {
	l.flowMu.Lock()
	defer l.flowMu.Unlock()
	for index, pending := range l.flowPending {
		if pending {
			l.flowPending[index] = false
			if l.flowPending[1-index] {
				wakeFlow(l.flowWake)
			}
			return l.flowFrames[index], true
		}
	}
	return wireFrame{}, false
}

func (c *mpCore) flowLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	f := c.flow
	for {
		select {
		case <-c.done:
			return
		case <-f.wake:
		case <-ticker.C:
		}
		leg := c.getLeg(0)
		if leg == nil || !leg.ready.Load() {
			continue
		}
		// Preserve hello + first DATA as the first early write. Flow requests
		// and probes may follow it, but must never start a lazy child themselves.
		if c.legCounters[0].txFrames.Load() == 0 && c.rxExpected.Load() == 0 && !c.receivedFIN.Load() {
			continue
		}
		now := time.Now()
		f.txMu.Lock()
		next := c.txSeq.Load()
		if !f.txWaiting && !f.txLast.IsZero() && now.Sub(f.txLast) >= 250*time.Millisecond && f.txLimit > next+1 {
			f.txEpoch++
			f.txLimit, f.txWanted = next+1, next+1
			f.txDirty = true
		}
		if f.txDirty {
			leg.queueFlow(wireFrame{typ: frameTypeWindowRequest, flow: flowMessage{Epoch: f.txEpoch, Next: next, Limit: f.txWanted}})
			f.txDirty = false
		}
		f.txMu.Unlock()

		f.rxMu.Lock()
		target := min(f.rxWanted, f.rxConsumed+f.rxCapacity)
		if target > f.rxLimit {
			granted := c.memory.reserveReceive(target-f.rxLimit, c.receiveSlotBytes())
			f.rxLimit += granted
			f.rxDirty = f.rxDirty || granted > 0
		}
		if f.rxDirty || !f.rxGapSince.IsZero() {
			message := flowMessage{Epoch: f.rxEpoch, Next: f.rxReceived, Limit: f.rxLimit, Leg1Bytes: f.rxLeg1Bytes}
			if f.rxFIN != nil && f.rxReceived == *f.rxFIN {
				message.Flags |= flowFlagFINAck
			}
			if !f.rxGapSince.IsZero() {
				message.Flags |= flowFlagGap
				message.GapAge = uint64(max(0, now.Sub(f.rxGapSince)))
				if f.rxLimit-f.rxConsumed >= f.rxCapacity || !c.memory.boosterAllowed() {
					message.Flags |= flowFlagPressure
				}
			}
			leg.queueFlow(wireFrame{typ: frameTypeWindow, flow: message})
			f.rxDirty = false
		}
		f.rxMu.Unlock()
		if c.active.Load() {
			c.scheduleProbes(now)
		}
	}
}
