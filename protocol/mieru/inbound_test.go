package mieru

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"

	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
)

func validMieruInboundOptions() option.MieruInboundOptions {
	return option.MieruInboundOptions{
		ListenOptions: option.ListenOptions{ListenPort: 8964},
		Transport:     "TCP",
		Users: []option.MieruUser{{
			Name:     "user1",
			Password: "password1",
		}},
	}
}

func TestValidateMieruInboundOptions(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*option.MieruInboundOptions)
		wantErr string
	}{
		{name: "tcp"},
		{name: "udp", mutate: func(o *option.MieruInboundOptions) { o.Transport = "UDP" }},
		{name: "invalid transport", mutate: func(o *option.MieruInboundOptions) { o.Transport = "QUIC" }, wantErr: "transport must be TCP or UDP"},
		{name: "no users", mutate: func(o *option.MieruInboundOptions) { o.Users = nil }, wantErr: "users is empty"},
		{name: "empty username", mutate: func(o *option.MieruInboundOptions) { o.Users[0].Name = "" }, wantErr: "username is empty"},
		{name: "empty password", mutate: func(o *option.MieruInboundOptions) { o.Users[0].Password = "" }, wantErr: "password is empty"},
		{name: "invalid traffic pattern", mutate: func(o *option.MieruInboundOptions) { o.TrafficPattern = "not-base64" }, wantErr: "traffic pattern"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := validMieruInboundOptions()
			if tt.mutate != nil {
				tt.mutate(&o)
			}
			err := validateMieruInboundOptions(o)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestBuildMieruServerConfig(t *testing.T) {
	o := validMieruInboundOptions()
	o.UserHintIsMandatory = true
	o.TrafficPattern = "GgQIARAK"
	config, users, err := buildMieruServerConfig(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if config == nil || config.Config == nil {
		t.Fatal("nil mieru server config")
	}
	if len(config.Config.PortBindings) != 1 {
		t.Fatalf("unexpected port binding count: %d", len(config.Config.PortBindings))
	}
	binding := config.Config.PortBindings[0]
	if binding.GetPort() != 8964 {
		t.Fatalf("unexpected port: %d", binding.GetPort())
	}
	if binding.GetProtocol() != mierupb.TransportProtocol_TCP {
		t.Fatalf("unexpected protocol: %v", binding.GetProtocol())
	}
	if len(config.Config.Users) != 1 || config.Config.Users[0].GetName() != "user1" || config.Config.Users[0].GetPassword() != "password1" {
		t.Fatalf("unexpected users: %+v", config.Config.Users)
	}
	if len(users) != 1 || users[0] != "user1" {
		t.Fatalf("unexpected user names: %v", users)
	}
	if config.Config.TrafficPattern == nil {
		t.Fatal("traffic pattern was not decoded")
	}
	if config.Config.AdvancedSettings == nil || !config.Config.AdvancedSettings.GetUserHintIsMandatory() {
		t.Fatal("user_hint_is_mandatory was not propagated")
	}
}

func TestBuildMieruServerConfigRequiresListenPort(t *testing.T) {
	o := validMieruInboundOptions()
	o.ListenPort = 0
	_, _, err := buildMieruServerConfig(context.Background(), o)
	if err == nil || !contains(err.Error(), "listen_port must be set") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildMieruServerConfigUDP(t *testing.T) {
	o := validMieruInboundOptions()
	o.Transport = "UDP"
	config, _, err := buildMieruServerConfig(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if got := config.Config.PortBindings[0].GetProtocol(); got != mierupb.TransportProtocol_UDP {
		t.Fatalf("unexpected protocol: %v", got)
	}
}

func TestMieruPacketConnReadPacketDomain(t *testing.T) {
	wire := buf.NewSize(2048)
	defer wire.Release()
	if _, err := wire.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	addr := mierumodel.AddrSpec{FQDN: "example.com", Port: 443}
	if err := addr.WriteToSocks5(wire); err != nil {
		t.Fatal(err)
	}
	if _, err := wire.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	pc := &capturePacketConn{readPacket: append([]byte(nil), wire.Bytes()...)}
	wrapped := &mieruPacketConn{PacketConn: pc}
	buffer := buf.NewSize(2048)
	defer buffer.Release()
	destination, err := wrapped.ReadPacket(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if !destination.IsFqdn() || destination.Fqdn != "example.com" || destination.Port != 443 {
		t.Fatalf("unexpected destination: %v", destination)
	}
	if string(buffer.Bytes()) != "payload" {
		t.Fatalf("unexpected payload: %q", buffer.Bytes())
	}
}

func TestMieruPacketConnWritePacketIPv4(t *testing.T) {
	pc := new(capturePacketConn)
	wrapped := &mieruPacketConn{PacketConn: pc}
	buffer := buf.As([]byte("payload"))
	defer buffer.Release()
	destination := M.Socksaddr{Addr: netip.MustParseAddr("203.0.113.7"), Port: 53}
	if err := wrapped.WritePacket(buffer, destination); err != nil {
		t.Fatal(err)
	}
	if len(pc.writtenPacket) < 4 || pc.writtenPacket[0] != 0 || pc.writtenPacket[1] != 0 || pc.writtenPacket[2] != 0 {
		t.Fatalf("missing SOCKS5 UDP RSV/FRAG prefix: %x", pc.writtenPacket)
	}
	decoded := buf.As(append([]byte(nil), pc.writtenPacket[3:]...))
	defer decoded.Release()
	var addr mierumodel.AddrSpec
	if err := addr.ReadFromSocks5(decoded); err != nil {
		t.Fatal(err)
	}
	if got, ok := netip.AddrFromSlice(addr.IP); !ok || got.Unmap() != destination.Addr || uint16(addr.Port) != destination.Port {
		t.Fatalf("unexpected encoded destination: %+v", addr)
	}
	if string(decoded.Bytes()) != "payload" {
		t.Fatalf("unexpected payload: %q", decoded.Bytes())
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

type capturePacketConn struct {
	readPacket    []byte
	writtenPacket []byte
}

func (c *capturePacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if c.readPacket == nil {
		return 0, nil, errors.New("no packet")
	}
	n := copy(p, c.readPacket)
	c.readPacket = nil
	return n, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}, nil
}

func (c *capturePacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.writtenPacket = append([]byte(nil), p...)
	return len(p), nil
}

func (c *capturePacketConn) Close() error                     { return nil }
func (c *capturePacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *capturePacketConn) SetDeadline(time.Time) error      { return nil }
func (c *capturePacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *capturePacketConn) SetWriteDeadline(time.Time) error { return nil }
