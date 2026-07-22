package main

import (
	"testing"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc"
)

func TestConfiguredDecodersRequiresExplicitIsolatedSimulation(t *testing.T) {
	for _, test := range []struct {
		name       string
		simulation bool
		codecs     bool
	}{
		{name: "no production decoder", simulation: false, codecs: false},
		{name: "codecs outside simulation", simulation: false, codecs: true},
		{name: "simulation without codecs", simulation: true, codecs: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := configuredDecoders(test.simulation, test.codecs); err == nil {
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
	if _, ok := decoders[cdc.SimulationProtobufCodec]; !ok {
		t.Fatal("simulation Protobuf decoder is not registered")
	}
}
