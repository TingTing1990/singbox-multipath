package multipath

import "time"

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
			capacity := c.preferredCapacityActivationState(now)
			bytesNow := c.ingressBytes.Load()
			if info, ok := activationAfterBytes(c.cfg, bytesNow, windowBase, now.Sub(windowStart)); ok {
				if !capacity.DeliveryReady {
					c.recordPreferredCapacityGateDecision(now, info, capacity, false)
				} else {
					c.recordPreferredCapacityGateDecision(now, info, capacity, true)
					c.preparePreferredCapacityActivation(now, &info, capacity)
					c.activate(info)
					return
				}
			}
			if (c.cfg.ThresholdBytesPS > 0 || (c.cfg.ActivationAfterBytes > 0 && c.cfg.ActivationAfterBytesMinBytesPS > 0)) && now.Sub(windowStart) >= c.cfg.ActivationWindow {
				delta := bytesNow - windowBase
				elapsed := now.Sub(windowStart)
				rate := uint64(0)
				if elapsed > 0 {
					rate = uint64(float64(delta) / elapsed.Seconds())
				}
				if c.cfg.ThresholdBytesPS > 0 && rate >= c.cfg.ThresholdBytesPS {
					info := activationInfo{
						Reason:           activationReasonThroughput,
						WindowBytes:      delta,
						RateBytesPS:      rate,
						ThresholdBytesPS: c.cfg.ThresholdBytesPS,
						Elapsed:          elapsed,
					}
					if !capacity.DeliveryReady {
						c.recordPreferredCapacityGateDecision(now, info, capacity, false)
					} else {
						c.recordPreferredCapacityGateDecision(now, info, capacity, true)
						c.preparePreferredCapacityActivation(now, &info, capacity)
						c.activate(info)
						return
					}
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
			c.stateMu.Lock()
			backlogBytes := primary.backlogBytes() + int64(c.tx.WriteNext-min(c.tx.Next, c.tx.WriteNext))
			c.stateMu.Unlock()
			if backlogBytes*5 >= c.cfg.QueueBytes*4 {
				if queueHighSince.IsZero() {
					queueHighSince = now
				} else if now.Sub(queueHighSince) >= c.cfg.ActivationWindow {
					info := activationInfo{
						Reason:           activationReasonLeg0Queue,
						BacklogBytes:     backlogBytes,
						QueueBytes:       c.cfg.QueueBytes,
						Elapsed:          now.Sub(queueHighSince),
						RequiredDuration: c.cfg.ActivationWindow,
					}
					if !capacity.DeliveryReady {
						c.recordPreferredCapacityGateDecision(now, info, capacity, false)
					} else {
						c.recordPreferredCapacityGateDecision(now, info, capacity, true)
						c.preparePreferredCapacityActivation(now, &info, capacity)
						c.activate(info)
						return
					}
				}
			} else {
				queueHighSince = time.Time{}
			}
		}
	}
}

func (c *mpCore) preferredCapacityActivationState(now time.Time) preferredCapacitySnapshot {
	controller := c.cfg.PreferredCapacity
	if controller == nil {
		return preferredCapacitySnapshot{DeliveryReady: true}
	}
	// A true failover state must not wait for a path the recovery policy has
	// already declared unavailable. Normal busy/backpressure is not failover.
	if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(0) {
		snapshot := controller.snapshot()
		snapshot.DeliveryReady = true
		snapshot.RecoveryBypass = true
		return snapshot
	}
	return controller.capacityState(now)
}

func (c *mpCore) preferredCapacityActivationReady(now time.Time) bool {
	return c.preferredCapacityActivationState(now).DeliveryReady
}

func preferredCapacityAuditTriggerIndex(reason activationReason) int {
	switch reason {
	case activationReasonBytes:
		return 0
	case activationReasonThroughput:
		return 1
	case activationReasonLeg0Queue:
		return 2
	default:
		return -1
	}
}

func (c *mpCore) recordPreferredCapacityGateDecision(now time.Time, info activationInfo, capacity preferredCapacitySnapshot, opened bool) {
	controller := c.cfg.PreferredCapacity
	if controller == nil {
		return
	}
	if !opened {
		index := preferredCapacityAuditTriggerIndex(info.Reason)
		if index >= 0 {
			if c.capacityAuditBlockedSeen[index] && c.capacityAuditBlockedWindow[index] == capacity.WindowSequence {
				return
			}
			c.capacityAuditBlockedSeen[index] = true
			c.capacityAuditBlockedWindow[index] = capacity.WindowSequence
		}
	}
	controller.recordGateDecision(now, info, capacity, opened, c.cfg.CapacityAuditSessionID, c.cfg.CapacityAuditDestination)
}

func (c *mpCore) preparePreferredCapacityActivation(now time.Time, info *activationInfo, capacity preferredCapacitySnapshot) {
	controller := c.cfg.PreferredCapacity
	if info == nil || controller == nil {
		return
	}
	// True recovery/failover may activate leg1 while preferred is explicitly
	// unavailable. That path bypasses the capacity gate and must not establish a
	// normal additive-protection epoch from a synthetic ready=true snapshot.
	if c.cfg.Recovery != nil && !c.cfg.Recovery.allows(0) {
		return
	}
	info.PreferredCapacityTargetBytesPS = capacity.TargetBytesPS
	info.PreferredCapacityRateBytesPS = capacity.DeliveryRate
	info.PreferredCapacityProtectedBytesPS = capacity.ProtectedBytesPS
	armedNow := controller.activateProtection(now, capacity)
	// activateProtection may establish the first additive-protection epoch. Read
	// the resulting protected rate for diagnostics so the activation record says
	// what the scheduler actually committed to preserve, not only the gate rate.
	postActivation := controller.snapshot()
	info.PreferredCapacityProtectedBytesPS = postActivation.ProtectedBytesPS
	if armedNow {
		controller.recordProtectionArmed(now, *info, postActivation, c.cfg.CapacityAuditSessionID, c.cfg.CapacityAuditDestination)
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
