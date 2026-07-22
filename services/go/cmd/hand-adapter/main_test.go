package main

import (
	"testing"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc"
)

func TestConfiguredDecodersRequiresExplicitIsolatedSimulation(t *testing.T) {
	for _, test := range []struct {
		name       string
		simulation bool
		fixture    bool
	}{
		{name: "no production decoder", simulation: false, fixture: false},
		{name: "fixture outside simulation", simulation: false, fixture: true},
		{name: "simulation without fixture", simulation: true, fixture: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := configuredDecoders(test.simulation, test.fixture); err == nil {
				t.Fatal("unsafe decoder mode was accepted")
			}
		})
	}

	decoders, err := configuredDecoders(true, true)
	if err != nil {
		t.Fatal(err)
	}
	decoder, ok := decoders[cdc.FixtureCodec]
	if !ok || decoder.Version() != cdc.FixtureCodec {
		t.Fatalf("simulation codec registry is wrong: %v", decoders)
	}
}
