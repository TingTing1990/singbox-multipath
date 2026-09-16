package multipath

import "time"

type recoveryPathStatus struct {
	Healthy  bool  `json:"healthy"`
	TCPAgeMS int64 `json:"tcp_reply_age_ms"`
	UDPAgeMS int64 `json:"udp_reply_age_ms"`
	StableMS int64 `json:"stable_ms"`
}
type recoveryStatus struct {
	Enabled           bool                  `json:"enabled"`
	FailoverTimeoutMS int64                 `json:"failover_timeout_ms"`
	FailbackDelayMS   int64                 `json:"failback_delay_ms"`
	TCPPath           byte                  `json:"tcp_path"`
	UDPPath           byte                  `json:"udp_path"`
	UDPPreferred      byte                  `json:"udp_preferred"`
	UsableMask        byte                  `json:"usable_mask"`
	Paths             [2]recoveryPathStatus `json:"paths"`
}

func (r *recoveryClient) snapshot(now time.Time) *recoveryStatus {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, mask, udp := r.policy.snapshot()
	s := &recoveryStatus{Enabled: true, FailoverTimeoutMS: r.timeout.Milliseconds(), FailbackDelayMS: r.delay.Milliseconds(), TCPPath: r.tcpPath(), UDPPath: udp, UDPPreferred: r.preferredUDP, UsableMask: mask}
	age := func(t time.Time) int64 {
		if t.IsZero() {
			return -1
		}
		return max(0, now.Sub(t).Milliseconds())
	}
	for id, h := range r.health {
		s.Paths[id] = recoveryPathStatus{Healthy: h.healthy, TCPAgeMS: age(h.lastTCP), UDPAgeMS: age(h.lastUDP), StableMS: age(h.since)}
	}
	return s
}
