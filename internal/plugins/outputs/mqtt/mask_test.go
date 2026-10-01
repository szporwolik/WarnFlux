package mqtt

import (
	"context"
	"testing"

	"github.com/szporwolik/WarnFlux/internal/core"
	"github.com/szporwolik/WarnFlux/internal/mqttpolicy"
	"github.com/szporwolik/WarnFlux/internal/plugin"
)

// TestPublishMaskSkipsEverything pins the policy gates: with an empty
// mask the output consumes journal changes, status heartbeats and
// information messages without any broker work (no connection attempt,
// no publish). Unmasked categories still go through the transport.
func TestPublishMaskSkipsEverything(t *testing.T) {
	t.Cleanup(func() { mqttpolicy.Set(uint32(mqttpolicy.CatAll)) })

	p, err := New(decodeConfig(t, "broker: tcp://localhost:1883\nclient_id: x\nqos: 1\n"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	o := p.(*Output)

	mqttpolicy.Set(0) // nothing publishes
	if err := o.Handle(context.Background(), core.EventChange{Event: core.HazardEvent{}}); err != nil {
		t.Fatalf("masked Handle = %v, want nil (consumed without publishing)", err)
	}
	if err := o.PublishStatus(context.Background(), pluginStatus()); err != nil {
		t.Fatalf("masked PublishStatus = %v, want nil", err)
	}
	if err := o.PublishInformation(context.Background(), core.InformationMessage{}); err != nil {
		t.Fatalf("masked PublishInformation = %v, want nil", err)
	}
}

func pluginStatus() plugin.Status {
	return plugin.Status{}
}
