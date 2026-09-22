package include

import (
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
)

func TestMieruInboundRegistered(t *testing.T) {
	registry := InboundRegistry()
	raw, ok := registry.CreateOptions(C.TypeMieru)
	if !ok {
		t.Fatal("mieru inbound is not registered")
	}
	if _, ok := raw.(*option.MieruInboundOptions); !ok {
		t.Fatalf("unexpected mieru inbound option type: %T", raw)
	}
}
