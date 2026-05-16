from __future__ import annotations

from pipeline.generator import GeneratorConfig, HandGenerator


def test_generator_emits_requested_hand_count():
    gen = HandGenerator(GeneratorConfig(n_hands=50, n_players=20, n_tables=4, n_colluding_pairs=4, seed=7))
    hands = list(gen.iter_hands())
    assert len(hands) == 50
    assert all("hand_id" in h for h in hands)
    assert all(len(h["players"]) == 6 for h in hands)


def test_generator_marks_some_suspicious_players():
    gen = HandGenerator(GeneratorConfig(n_hands=200, n_players=40, n_tables=4, n_colluding_pairs=10, seed=11))
    hands = list(gen.iter_hands())
    suspicious_flags = [p["is_suspicious"] for h in hands for p in h["players"]]
    assert any(suspicious_flags), "Expected at least one suspicious player flag among 200 hands"
