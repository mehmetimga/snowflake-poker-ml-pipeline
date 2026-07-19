from __future__ import annotations

import pytest

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


def test_pokerkit_generator_is_deterministic_and_settles_real_pots():
    config = GeneratorConfig(
        n_hands=10,
        n_players=24,
        n_tables=3,
        n_colluding_pairs=6,
        seed=91,
        dataset_split="test",
    )
    first = list(HandGenerator(config).iter_hands())
    second = list(HandGenerator(config).iter_hands())
    assert first == second

    for hand in first:
        assert hand["generator"] == "pokerkit"
        assert hand["dataset_split"] == "test"
        assert hand["hand_id"].startswith("TEST-H-")
        cards = hand["board"] + [
            card
            for player in hand["players"]
            for card in player["hole_cards"].split()
        ]
        assert len(cards) == len(set(cards))
        assert sum(player["won_amount"] for player in hand["players"]) == pytest.approx(
            hand["pot_size"]
        )
        assert [action["sequence_no"] for action in hand["actions"]] == list(
            range(len(hand["actions"]))
        )
