"""Build a homogeneous player-player graph from PAIR_STATS."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import networkx as nx
import numpy as np
import pandas as pd
import torch
from torch_geometric.data import Data

from pipeline.warehouse import Warehouse


@dataclass
class PlayerGraph:
    data: Data
    node_to_id: list[str]
    id_to_node: dict[str, int]
    edge_features: np.ndarray  # per-edge [hands_together, chip_transfer_ratio, soft_play, fold_benefit, showdown_avoid]


def _node_features(warehouse: Warehouse, node_ids: Sequence[str]) -> np.ndarray:
    players = warehouse.fetch_df("SELECT player_id, won_amount FROM RAW_PLAYERS")
    if players.empty:
        return np.zeros((len(node_ids), 4), dtype=np.float32)
    agg = players.groupby("player_id").agg(
        hands_played=("won_amount", "size"),
        won_total=("won_amount", "sum"),
        won_mean=("won_amount", "mean"),
        won_std=("won_amount", "std"),
    ).fillna(0.0)
    feats = []
    for pid in node_ids:
        if pid in agg.index:
            row = agg.loc[pid]
            feats.append([row["hands_played"], row["won_total"], row["won_mean"], row["won_std"]])
        else:
            feats.append([0.0, 0.0, 0.0, 0.0])
    arr = np.array(feats, dtype=np.float32)
    mean = arr.mean(axis=0, keepdims=True)
    std = arr.std(axis=0, keepdims=True) + 1e-6
    return (arr - mean) / std


def build_player_graph(warehouse: Warehouse) -> PlayerGraph | None:
    pair_stats = warehouse.fetch_df("SELECT * FROM PAIR_STATS")
    if pair_stats.empty:
        return None

    players = sorted(set(pair_stats["player_a"]) | set(pair_stats["player_b"]))
    id_to_node = {pid: i for i, pid in enumerate(players)}

    src, dst, edge_attr = [], [], []
    for _, row in pair_stats.iterrows():
        a = id_to_node[row["player_a"]]
        b = id_to_node[row["player_b"]]
        attrs = [
            row["hands_together"],
            row["chip_transfer_ratio"],
            row["soft_play_frequency"],
            row["fold_benefit_ratio"],
            row["showdown_avoidance_rate"],
        ]
        # Undirected: emit both directions
        src.extend([a, b])
        dst.extend([b, a])
        edge_attr.extend([attrs, attrs])

    edge_index = torch.tensor([src, dst], dtype=torch.long)
    edge_attr_arr = np.array(edge_attr, dtype=np.float32)
    x = torch.tensor(_node_features(warehouse, players), dtype=torch.float32)
    data = Data(x=x, edge_index=edge_index, edge_attr=torch.tensor(edge_attr_arr))

    return PlayerGraph(data=data, node_to_id=players, id_to_node=id_to_node, edge_features=edge_attr_arr)
