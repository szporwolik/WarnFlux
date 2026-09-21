package plugin

import (
	"context"
	"testing"

	"gopkg.in/yaml.v3"

	"warnflux/internal/core"
)

func sourceFactoryFor(t *testing.T, p SourcePlugin) SourceFactory {
	t.Helper()
	return func(_ *yaml.Node) (SourcePlugin, error) { return p, nil }
}

func outputFactoryFor(t *testing.T, p OutputPlugin) OutputFactory {
	t.Helper()
	return func(_ *yaml.Node) (OutputPlugin, error) { return p, nil }
}

type stubSource struct {
	name string
	run  func(ctx context.Context, emit Emitter) error
}

func (s *stubSource) Name() string { return s.name }

func (s *stubSource) Run(ctx context.Context, emit Emitter) error {
	if s.run != nil {
		return s.run(ctx, emit)
	}
	<-ctx.Done()
	return nil
}

func TestRegistrySourceRegisterAndLookup(t *testing.T) {
	reg := NewRegistry()
	p := &stubSource{name: "stub"}
	if err := reg.RegisterSource("stub", sourceFactoryFor(t, p)); err != nil {
		t.Fatalf("RegisterSource: %v", err)
	}
	got, err := reg.Source("stub")
	if err != nil {
		t.Fatalf("Source lookup: %v", err)
	}
	built, err := got(nil)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if built.Name() != "stub" {
		t.Errorf("built plugin = %q", built.Name())
	}
}

func TestRegistryOutputRegisterAndLookup(t *testing.T) {
	reg := NewRegistry()
	p := stubOutput{name: "stub"}
	if err := reg.RegisterOutput("stub", outputFactoryFor(t, p)); err != nil {
		t.Fatalf("RegisterOutput: %v", err)
	}
	if _, err := reg.Output("stub"); err != nil {
		t.Fatalf("Output lookup: %v", err)
	}
}

func TestRegistryDuplicateRejected(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterSource("dup", sourceFactoryFor(t, &stubSource{name: "a"})); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := reg.RegisterSource("dup", sourceFactoryFor(t, &stubSource{name: "b"})); err == nil {
		t.Fatal("duplicate source registration must fail")
	}
	if err := reg.RegisterOutput("dup", outputFactoryFor(t, stubOutput{})); err != nil {
		t.Fatalf("output registration: %v", err)
	}
	if err := reg.RegisterOutput("dup", outputFactoryFor(t, stubOutput{})); err == nil {
		t.Fatal("duplicate output registration must fail")
	}
}

func TestRegistryUnknownType(t *testing.T) {
	reg := NewRegistry()
	if _, err := reg.Source("nope"); err == nil {
		t.Fatal("unknown source type must fail")
	}
	if _, err := reg.Output("nope"); err == nil {
		t.Fatal("unknown output type must fail")
	}
}

func TestRegistryRejectsEmptyTypeAndNilFactory(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterSource("", sourceFactoryFor(t, &stubSource{})); err == nil {
		t.Fatal("empty source type must fail")
	}
	if err := reg.RegisterSource("x", nil); err == nil {
		t.Fatal("nil source factory must fail")
	}
	if err := reg.RegisterOutput("", outputFactoryFor(t, stubOutput{})); err == nil {
		t.Fatal("empty output type must fail")
	}
	if err := reg.RegisterOutput("x", nil); err == nil {
		t.Fatal("nil output factory must fail")
	}
}

type stubOutput struct {
	name   string
	handle func(ctx context.Context, change core.EventChange) error
}

func (o stubOutput) Name() string { return o.name }

func (o stubOutput) Handle(ctx context.Context, change core.EventChange) error {
	if o.handle != nil {
		return o.handle(ctx, change)
	}
	return nil
}
