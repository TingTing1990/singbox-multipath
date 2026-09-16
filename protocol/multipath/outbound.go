package multipath

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.MultipathOutboundOptions](registry, C.TypeMultipath, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	ctx              context.Context
	manager          adapter.OutboundManager
	logger           log.ContextLogger
	tags             []string
	children         []adapter.Outbound
	udpTag           string
	udpOutbound      adapter.Outbound
	aggregation      M.Socksaddr
	tcpFastOpen      bool
	cfg              coreConfig
	handshakeTimeout time.Duration
	statusFile       string
	status           *outboundStatus
	failoverEnabled  bool
	failoverTimeout  time.Duration
	failbackDelay    time.Duration
	recovery         *recoveryClient
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MultipathOutboundOptions) (adapter.Outbound, error) {
	if len(options.DeprecatedBandwidthMbps) > 0 {
		logger.Warn("bandwidth_mbps is obsolete and ignored; multipath uses automatic delivery-based scheduling; remove this field from the configuration")
	}
	if len(options.Outbounds) != 2 {
		return nil, E.New("multipath PoC requires exactly 2 outbounds")
	}
	if options.Server == "" || options.ServerPort == 0 {
		return nil, E.New("missing multipath server/server_port")
	}
	activationAfterBytes := options.ActivationAfterBytes.Value()
	preferred := options.Preferred
	if preferred == "" {
		preferred = options.Outbounds[0]
	}
	preferredIndex := slices.Index(options.Outbounds, preferred)
	if preferredIndex < 0 {
		return nil, E.New("preferred outbound is not in outbounds: ", preferred)
	}
	tags := append([]string(nil), options.Outbounds...)
	if preferredIndex != 0 {
		tags[0], tags[preferredIndex] = tags[preferredIndex], tags[0]
	}
	udpTag := options.UDPOutbound
	if udpTag == "" {
		udpTag = preferred
	}
	failoverTimeout, failbackDelay, err := recoveryDurations(time.Duration(options.FailoverTimeout), time.Duration(options.FailbackDelay))
	if err != nil {
		return nil, err
	}
	if options.FailoverEnabled && !slices.Contains(tags, udpTag) {
		return nil, E.New("failover requires udp_outbound to be one of the two legs")
	}
	threshold := resolveActivationThreshold(options.ActivationThresholdMbps, activationAfterBytes)
	window := time.Duration(options.ActivationWindow)
	if window <= 0 {
		window = time.Second
	}
	chunkSize := int(options.ChunkSize)
	if chunkSize == 0 {
		chunkSize = 64 * 1024
	}
	if chunkSize < 1024 || chunkSize > maxFramePayload {
		return nil, E.New("invalid chunk_size")
	}
	queueFrames := int(options.QueueFrames)
	if queueFrames == 0 {
		queueFrames = 256
	}
	if queueFrames < 8 || queueFrames > 4096 {
		return nil, E.New("invalid queue_frames")
	}
	queueBytes := int64(chunkSize) * int64(queueFrames)
	if queueBytes > maxQueueBytes {
		return nil, E.New("chunk_size * queue_frames exceeds 64 MiB")
	}
	memoryLimit, automaticMemoryLimit, memoryErr := resolveMemoryLimit(options.MemoryLimit.Value())
	if memoryErr != nil {
		logger.Warn("detect available memory for multipath: ", memoryErr, "; using 256 MiB fallback")
	}
	minimumMemory := minimumSessionMemory(coreConfig{QueueFrames: queueFrames, ChunkSize: chunkSize})
	if memoryLimit < minimumMemory {
		return nil, E.New("memory_limit is too small for one multipath session: ", memoryLimit, " < ", minimumMemory)
	}
	memory := newMemoryBudget(memoryLimit, automaticMemoryLimit)
	maxReorderFrames := int(options.MaxReorderFrames)
	if maxReorderFrames != 0 && (maxReorderFrames < 64 || maxReorderFrames > 65536) {
		return nil, E.New("invalid max_reorder_frames")
	}
	maxReorderBufferBytes := int64(options.MaxReorderBytes)
	if maxReorderBufferBytes == 0 {
		maxReorderBufferBytes = automaticBufferLimit(memory, chunkSize)
	}
	if maxReorderBufferBytes < int64(chunkSize) || maxReorderBufferBytes > maxReorderBytes {
		return nil, E.New("invalid max_reorder_bytes")
	}
	replayBytes := int64(options.Leg1ReplayBytes)
	if replayBytes == 0 {
		replayBytes = automaticBufferLimit(memory, chunkSize)
	}
	if replayBytes < int64(chunkSize) || replayBytes > maxReplayBytes {
		return nil, E.New("invalid leg1_replay_bytes")
	}
	replayTimeout := time.Duration(options.Leg1ReplayTimeout)
	if replayTimeout != 0 && (replayTimeout < 100*time.Millisecond || replayTimeout > 5*time.Minute) {
		return nil, E.New("invalid leg1_replay_timeout")
	}
	handshakeTimeout := time.Duration(options.HandshakeTimeout)
	if handshakeTimeout <= 0 {
		handshakeTimeout = 10 * time.Second
	}
	if handshakeTimeout < time.Second || handshakeTimeout > time.Minute {
		return nil, E.New("invalid handshake_timeout")
	}
	dependencies := append([]string(nil), options.Outbounds...)
	if !slices.Contains(dependencies, udpTag) {
		dependencies = append(dependencies, udpTag)
	}
	return &Outbound{
		Adapter:          outbound.NewAdapter(C.TypeMultipath, tag, []string{N.NetworkTCP, N.NetworkUDP}, dependencies),
		ctx:              ctx,
		manager:          service.FromContext[adapter.OutboundManager](ctx),
		logger:           logger,
		tags:             tags,
		udpTag:           udpTag,
		aggregation:      M.ParseSocksaddrHostPort(options.Server, options.ServerPort),
		tcpFastOpen:      options.TCPFastOpen,
		handshakeTimeout: handshakeTimeout,
		statusFile:       options.StatusFile,
		failoverEnabled:  options.FailoverEnabled,
		failoverTimeout:  failoverTimeout,
		failbackDelay:    failbackDelay,
		cfg: coreConfig{
			AggregationEnabled:             options.AggregationEnabled == nil || *options.AggregationEnabled,
			ActivationOnQueue:              options.ActivationOnQueue == nil || *options.ActivationOnQueue,
			ChunkSize:                      chunkSize,
			QueueFrames:                    queueFrames,
			QueueBytes:                     queueBytes,
			ThresholdBytesPS:               threshold,
			ActivationAfterBytes:           activationAfterBytes,
			ActivationAfterBytesMinBytesPS: uint64(options.ActivationAfterBytesMinMbps) * 1000 * 1000 / 8,
			ActivationWindow:               window,
			MaxReorderFrames:               maxReorderFrames,
			MaxReorderBytes:                maxReorderBufferBytes,
			ReplayBytes:                    replayBytes,
			ReplayTimeout:                  replayTimeout,
			Memory:                         memory,
		},
	}, nil
}

func (o *Outbound) Start() error {
	o.children = make([]adapter.Outbound, len(o.tags))
	for index, tag := range o.tags {
		child, loaded := o.manager.Outbound(tag)
		if !loaded {
			return E.New("multipath child outbound not found: ", tag)
		}
		if !slices.Contains(child.Network(), N.NetworkTCP) {
			return E.New("multipath child does not support TCP: ", tag)
		}
		o.children[index] = child
		if o.failoverEnabled && !slices.Contains(child.Network(), N.NetworkUDP) {
			return E.New("failover requires TCP and UDP on both legs: ", tag)
		}
	}
	udpOutbound, loaded := o.manager.Outbound(o.udpTag)
	if !loaded {
		return E.New("multipath UDP outbound not found: ", o.udpTag)
	}
	if !slices.Contains(udpOutbound.Network(), N.NetworkUDP) {
		return E.New("multipath UDP outbound does not support UDP: ", o.udpTag)
	}
	o.udpOutbound = udpOutbound
	if o.failoverEnabled {
		var err error
		o.recovery, err = newRecoveryClient(o)
		if err != nil {
			return err
		}
		o.cfg.Recovery = &o.recovery.policy
	}
	if o.statusFile != "" {
		var legTypes [2]string
		for index, child := range o.children {
			legTypes[index] = child.Type()
		}
		o.status = newOutboundStatus(o.statusFile, outboundStatusConfig{
			tag:              o.Tag(),
			aggregation:      o.aggregation.String(),
			udpOutbound:      o.udpTag,
			tcpFastOpen:      o.tcpFastOpen,
			handshakeTimeout: o.handshakeTimeout,
			legTags:          [2]string{o.tags[0], o.tags[1]},
			legTypes:         legTypes,
			cfg:              o.cfg,
			recovery:         o.recovery,
		})
		o.status.start(o.ctx, func(err error) {
			o.logger.Warn("write multipath status: ", err)
		})
	}
	o.cfg.Memory.startLogging(o.ctx, o.logger, "client")
	if o.recovery != nil {
		o.recovery.start()
	}
	return nil
}

func (o *Outbound) Close() error {
	if o.recovery != nil {
		o.recovery.close()
	}
	return o.status.close()
}

func (o *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkUDP:
		if o.recovery != nil {
			conn, err := o.recovery.listenPacket(ctx, destination)
			if err != nil {
				return nil, err
			}
			return bufio.NewBindPacketConn(conn, destination), nil
		}
		conn, err := o.udpOutbound.DialContext(ctx, network, destination)
		if err != nil {
			return conn, err
		}
		return o.trackUDPConnection(conn), nil
	case N.NetworkTCP:
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	if len(o.children) != 2 {
		return nil, E.New("multipath outbound is not started")
	}
	if o.recovery != nil {
		return o.dialRecovery(ctx, destination)
	}
	if o.tcpFastOpen {
		return o.dialTCPFastOpen(ctx, destination)
	}
	sessionID, err := newSessionID()
	if err != nil {
		return nil, E.Cause(err, "generate multipath session id")
	}
	destinationString := destination.String()
	primaryConn, err := o.children[0].DialContext(ctx, N.NetworkTCP, o.aggregation)
	if err != nil {
		return nil, E.Cause(err, "dial multipath preferred leg ", o.tags[0])
	}
	err = o.clientHandshake(ctx, primaryConn, helloMessage{
		Session:       sessionID,
		LegID:         0,
		RequestStatus: o.statusFile != "",
		ChunkSize:     uint32(o.cfg.ChunkSize),
		Destination:   destinationString,
	})
	if err != nil {
		if o.status != nil {
			o.status.recordLegError(0, string(legFailureHandshake), destinationString, statusSessionID(sessionID), 0, err, time.Now())
		}
		primaryConn.Close()
		return nil, E.Cause(err, "multipath preferred handshake")
	}
	cfg := o.connectionCoreConfig(ctx, destination, sessionID)
	core, appConn, err := newCoreWithError(ctx, cfg)
	if err != nil {
		primaryConn.Close()
		return nil, E.Cause(err, "create multipath session")
	}
	if _, err = core.addLeg(0, primaryConn, nil); err != nil {
		appConn.Close()
		primaryConn.Close()
		return nil, err
	}
	statusSession := o.registerStatusSession(sessionID, destinationString, core, leg1PhaseConnecting)
	o.logger.InfoContext(ctx, "multipath connection to ", destination, " via preferred ", o.tags[0])
	go o.joinSecondary(core, sessionID, uint32(cfg.ChunkSize), destinationString, statusSession)
	return appConn, nil
}

func (o *Outbound) dialTCPFastOpen(ctx context.Context, destination M.Socksaddr) (net.Conn, error) {
	sessionID, err := newSessionID()
	if err != nil {
		return nil, E.Cause(err, "generate multipath session id")
	}
	destinationString := destination.String()
	primaryConn, err := o.children[0].DialContext(ctx, N.NetworkTCP, o.aggregation)
	if err != nil {
		return nil, E.Cause(err, "dial multipath preferred leg ", o.tags[0])
	}
	message := helloMessage{
		Session:       sessionID,
		LegID:         0,
		RequestStatus: o.statusFile != "",
		ChunkSize:     uint32(o.cfg.ChunkSize),
		Destination:   destinationString,
	}
	fastOpenConn, err := newClientFastOpenConn(primaryConn, message, o.clientHandshakeDeadline(ctx))
	if err != nil {
		primaryConn.Close()
		return nil, E.Cause(err, "prepare multipath preferred fast open")
	}
	cfg := o.connectionCoreConfig(ctx, destination, sessionID)
	core, appConn, err := newCoreWithError(ctx, cfg)
	if err != nil {
		fastOpenConn.Close()
		return nil, E.Cause(err, "create multipath session")
	}
	readResponse := func(conn net.Conn) error {
		if waitErr := fastOpenConn.waitStarted(); waitErr != nil {
			return waitErr
		}
		response, responseErr := readHelloResponse(conn)
		if responseErr != nil {
			return responseErr
		}
		if response.ChunkSize != message.ChunkSize {
			return E.New("multipath server changed accepted chunk size from ", message.ChunkSize, " to ", response.ChunkSize)
		}
		return conn.SetDeadline(time.Time{})
	}
	if _, err = core.addLegWithReadPreamble(0, fastOpenConn, nil, readResponse); err != nil {
		appConn.Close()
		fastOpenConn.Close()
		return nil, err
	}
	statusSession := o.registerStatusSession(sessionID, destinationString, core, leg1PhaseWaiting)
	o.logger.InfoContext(ctx, "multipath fast-open connection to ", destination, " via preferred ", o.tags[0])
	go func() {
		if startErr := fastOpenConn.waitStarted(); startErr == nil {
			o.joinSecondary(core, sessionID, uint32(cfg.ChunkSize), destinationString, statusSession)
		}
	}()
	return &earlyLogicalConn{
		Conn:    appConn,
		core:    core,
		primary: fastOpenConn,
	}, nil
}

func (o *Outbound) connectionCoreConfig(ctx context.Context, destination M.Socksaddr, sessionID [16]byte) coreConfig {
	cfg := o.cfg
	cfg.OnProtocolError = func(err error) {
		o.logger.ErrorContext(ctx, "multipath protocol error for ", destination, ": ", err)
	}
	cfg.OnLeg1Active = func(info activationInfo, reconnect bool) {
		o.logger.InfoContext(
			ctx,
			"multipath leg1 joined data path: side=client destination=", destination,
			" outbound=", o.tags[1],
			" reconnect=", reconnect,
			" ", info.String(),
		)
	}
	cfg.OnLegFailure = func(legID uint8, stage legFailureStage, err error) {
		if o.status != nil {
			o.status.recordLegError(legID, string(stage), destination.String(), statusSessionID(sessionID), 0, err, time.Now())
		}
	}
	return cfg
}

func (o *Outbound) registerStatusSession(sessionID [16]byte, destination string, core *mpCore, phase int32) *statusSession {
	if o.status == nil {
		return nil
	}
	return o.status.addSession(sessionID, destination, core, phase)
}

func (o *Outbound) clientHandshake(ctx context.Context, conn net.Conn, message helloMessage) error {
	deadline := o.clientHandshakeDeadline(ctx)
	deadlineSet, err := setClientHandshakeDeadline(conn, deadline)
	if err != nil {
		return err
	}
	if err = writeHello(conn, message); err != nil {
		return err
	}
	if !deadlineSet {
		if err = conn.SetDeadline(deadline); err != nil {
			return err
		}
	}
	response, err := readHelloResponse(conn)
	if err != nil {
		return err
	}
	if response.ChunkSize != message.ChunkSize {
		return E.New("multipath server changed accepted chunk size from ", message.ChunkSize, " to ", response.ChunkSize)
	}
	return conn.SetDeadline(time.Time{})
}

func (o *Outbound) clientHandshakeDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(o.handshakeTimeout)
	if contextDeadline, loaded := ctx.Deadline(); loaded && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return deadline
}

func setClientHandshakeDeadline(conn net.Conn, deadline time.Time) (bool, error) {
	err := conn.SetDeadline(deadline)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrInvalid) && needsHandshakeForWrite(conn) {
		return false, nil
	}
	return false, err
}

func needsHandshakeForWrite(conn net.Conn) bool {
	if N.NeedHandshakeForWrite(conn) {
		return true
	}
	// slowOpenConn exposes its lazy state through NeedHandshake. Trackers hide
	// that method on the outer net.Conn but expose it through Upstream.
	earlyConn, loaded := common.Cast[interface{ NeedHandshake() bool }](conn)
	return loaded && earlyConn.NeedHandshake()
}

func (o *Outbound) joinSecondary(core *mpCore, sessionID [16]byte, chunkSize uint32, destination string, statusSession *statusSession) {
	ctx := core.Context()
	for {
		select {
		case <-core.Done():
			return
		default:
		}
		statusSession.beginLeg1Attempt()
		stage := "secondary_dial"
		attemptCtx, cancel := context.WithTimeout(ctx, o.handshakeTimeout)
		conn, err := o.children[1].DialContext(attemptCtx, N.NetworkTCP, o.aggregation)
		if err == nil {
			stage = "secondary_handshake"
			err = o.clientHandshake(attemptCtx, conn, helloMessage{
				Session:       sessionID,
				LegID:         1,
				RequestStatus: o.statusFile != "",
				ChunkSize:     chunkSize,
				Destination:   destination,
			})
		}
		var leg *mpLeg
		if err == nil {
			stage = "secondary_attach"
			leg, err = core.addLeg(1, conn, nil)
		}
		cancel()
		if err == nil {
			statusSession.setLeg1Phase(leg1PhaseReady)
			o.logger.InfoContext(ctx, "multipath secondary leg ready via ", o.tags[1])
			select {
			case <-core.Done():
				return
			case <-leg.Done():
				if leg.peerTerminal.Load() {
					<-core.Done()
					return
				}
				select {
				case <-core.Done():
					return
				default:
				}
				statusSession.setLeg1Phase(leg1PhaseRetrying)
				o.logger.WarnContext(ctx, "multipath secondary leg lost; retrying via ", o.tags[1])
			}
			continue
		}
		if conn != nil {
			conn.Close()
		}
		select {
		case <-core.Done():
			return
		default:
		}
		statusSession.setLeg1Phase(leg1PhaseRetrying)
		reason, rejected := helloRejectReasonFromError(err)
		if rejected && (reason == helloRejectSessionUnavailable || reason == helloRejectLegUnavailable) {
			o.logger.DebugContext(ctx, "multipath secondary leg deferred: ", err)
		} else {
			statusSession.recordLegError(1, stage, err)
			o.logger.WarnContext(ctx, "multipath secondary leg failed: ", err)
		}
		retryTimer := time.NewTimer(2 * time.Second)
		select {
		case <-core.Done():
			retryTimer.Stop()
			return
		case <-retryTimer.C:
		}
	}
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if o.recovery != nil {
		return o.recovery.listenPacket(ctx, destination)
	}
	if o.udpOutbound == nil {
		return nil, E.New("multipath outbound is not started")
	}
	conn, err := o.udpOutbound.ListenPacket(ctx, destination)
	if err != nil {
		return conn, err
	}
	return o.trackUDPPacketConnection(conn), nil
}

func (o *Outbound) trackUDPConnection(conn net.Conn) net.Conn {
	if o.status == nil {
		return conn
	}
	return bufio.NewCounterConn(conn, []N.CountFunc{o.status.countUDPRX}, []N.CountFunc{o.status.countUDPTX})
}

func (o *Outbound) trackUDPPacketConnection(conn net.PacketConn) net.PacketConn {
	if o.status == nil {
		return conn
	}
	return bufio.NewNetPacketConn(bufio.NewCounterPacketConn(
		bufio.NewPacketConn(conn),
		[]N.CountFunc{o.status.countUDPRX},
		[]N.CountFunc{o.status.countUDPTX},
	))
}
