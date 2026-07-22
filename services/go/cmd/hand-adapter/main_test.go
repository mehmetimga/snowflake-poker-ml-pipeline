package main

import (
	"context"
	"testing"

	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc"
	"github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/stream"
)

type commandCommitter struct {
	calls int
}

func (committer *commandCommitter) Commit(
	_ context.Context,
	_ []stream.RecordRef,
) error {
	committer.calls++
	return nil
}

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

func TestCommitFailureInjectionIsSimulationOnlyAndFailsExactlyOnce(t *testing.T) {
	if err := validateSimulationFailureInjection(false, true); err == nil {
		t.Fatal("production mode accepted commit failure injection")
	}
	if err := validateSimulationFailureInjection(true, true); err != nil {
		t.Fatal(err)
	}
	delegate := &commandCommitter{}
	committer := &failFirstCommitter{delegate: delegate}
	records := []stream.RecordRef{{Topic: cdc.SimulationSourceTopic, Offset: 7}}
	if err := committer.Commit(context.Background(), records); err == nil {
		t.Fatal("first commit was not rejected")
	}
	if delegate.calls != 0 {
		t.Fatal("injected failure reached the real committer")
	}
	if err := committer.Commit(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if delegate.calls != 1 {
		t.Fatalf("second commit was not delegated: %d", delegate.calls)
	}
}
