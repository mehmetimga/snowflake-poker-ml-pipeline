from __future__ import annotations

from pipeline.rules import RuleEngine
from pipeline.rules.types import PlayerHandStats


def _stats(**overrides) -> PlayerHandStats:
    base = dict(
        hand_id="H-1",
        player_id="P-1",
        position="BTN",
        stack_start=100.0,
        total_invested=0.0,
        preflop_raises_player=0,
        preflop_raises_hand=0,
        players_to_flop=0,
        saw_flop=False,
        saw_river=False,
        folded_street=None,
        preflop_invested=0.0,
        postflop_invested=0.0,
        preflop_aggression=0.0,
        postflop_aggression=0.0,
    )
    base.update(overrides)
    return PlayerHandStats(**base)


def test_high_invest_fold_triggers():
    eng = RuleEngine()
    stats = _stats(total_invested=40.0, folded_street="flop")
    flags = eng.check_player_hand(stats, num_players_in_hand=4)
    assert flags.flag_eligible
    assert flags.flag_high_invest_fold
    assert flags.rule_score >= 25.0


def test_post_ofold_commit_triggers_with_flop_seen():
    eng = RuleEngine()
    stats = _stats(total_invested=60.0, saw_flop=True, folded_street="turn")
    flags = eng.check_player_hand(stats, num_players_in_hand=5)
    assert flags.flag_post_ofold_commit
    assert flags.rule_score > 50.0


def test_no_eligibility_when_too_few_players():
    eng = RuleEngine()
    stats = _stats(total_invested=60.0, folded_street="preflop", preflop_invested=60.0)
    flags = eng.check_player_hand(stats, num_players_in_hand=2)
    assert not flags.flag_eligible
    assert flags.rule_score == 0.0


def test_position_anomaly_fires_in_late_position():
    eng = RuleEngine()
    stats = _stats(preflop_aggression=0.0, postflop_aggression=2.0)
    flags = eng.check_player_hand(stats, num_players_in_hand=4)
    assert flags.flag_position_anomaly
