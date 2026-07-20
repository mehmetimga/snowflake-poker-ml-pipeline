"""Local ONNX scorer for the versioned pair-model artifact contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pipeline.ml.pair_model import PairPreprocessor, PlattCalibrator


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PairOnnxScorer:
    """Score complete hands locally with the same contract used by Triton."""

    def __init__(self, model_dir: Path | str) -> None:
        import onnxruntime as ort

        self.model_dir = Path(model_dir).resolve()
        artifact_manifest = json.loads(
            (self.model_dir / "artifact_manifest.json").read_text()
        )
        for relative, expected in artifact_manifest["artifacts"].items():
            path = self.model_dir / relative
            if not path.is_file() or _sha256(path) != expected:
                raise ValueError(f"model artifact hash mismatch: {relative}")

        self.contract = json.loads(
            (self.model_dir / "scoring_contract.json").read_text()
        )
        self.preprocessor = PairPreprocessor.from_dict(
            json.loads((self.model_dir / self.contract["input"]["preprocessing"]).read_text())
        )
        self.calibrator = PlattCalibrator.from_dict(
            json.loads((self.model_dir / self.contract["calibration"]).read_text())
        )
        self.policy = json.loads(
            (self.model_dir / self.contract["decision_policy"]).read_text()
        )
        self.input_name = self.contract["input"]["name"]
        self.output_name = self.contract["output"]["name"]
        self.positive_class_index = int(
            self.contract["output"]["positive_class_index"]
        )
        self.session = ort.InferenceSession(
            str(self.model_dir / "model.onnx"), providers=["CPUExecutionProvider"]
        )
        session_inputs = {item.name for item in self.session.get_inputs()}
        session_outputs = {item.name for item in self.session.get_outputs()}
        if self.input_name not in session_inputs or self.output_name not in session_outputs:
            raise ValueError("scoring contract does not match ONNX inputs/outputs")
        if list(self.preprocessor.output_columns) != self.contract["input"][
            "ordered_features"
        ]:
            raise ValueError("preprocessing output order does not match scoring contract")

    def score_pairs(self, frame: pd.DataFrame) -> pd.DataFrame:
        expected_version = self.contract["feature_definition_version"]
        if "feature_definition_version" in frame:
            versions = set(frame["feature_definition_version"].dropna().astype(str))
            if versions != {expected_version}:
                raise ValueError(
                    f"feature-definition mismatch: expected {expected_version}, got {versions}"
                )
        matrix = self.preprocessor.transform(frame)
        raw = np.asarray(
            self.session.run(
                [self.output_name],
                {self.input_name: matrix.astype(np.float32, copy=False)},
            )[0],
            dtype=np.float64,
        )[:, self.positive_class_index]
        calibrated = self.calibrator.predict(raw)
        threshold = float(self.policy["threshold"])
        identity_columns = [
            column
            for column in ("event_id", "hand_id", "pair_key", "player_a", "player_b")
            if column in frame
        ]
        result = frame[identity_columns].reset_index(drop=True).copy()
        result["raw_probability"] = raw
        result["calibrated_probability"] = calibrated
        result["alert"] = calibrated >= threshold
        result["model_name"] = self.contract["model_name"]
        result["model_run_id"] = self.contract["run_id"]
        result["decision_threshold"] = threshold
        return result

    def score_complete_hands(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if "hand_id" not in frame:
            raise ValueError("complete-hand scoring requires hand_id")
        expected_pairs = int(
            self.contract["batching"]["expected_pairs_per_six_player_hand"]
        )
        pair_counts = frame.groupby("hand_id").size()
        invalid = pair_counts[pair_counts != expected_pairs]
        if not invalid.empty:
            raise ValueError(
                f"expected {expected_pairs} pairs per hand; invalid={invalid.to_dict()}"
            )
        pair_scores = self.score_pairs(frame)
        hand_scores = (
            pair_scores.groupby("hand_id", as_index=False)
            .agg(
                risk_probability=("calibrated_probability", "max"),
                alert=("alert", "max"),
                scored_pairs=("pair_key", "count"),
            )
            .sort_values("hand_id", kind="mergesort")
            .reset_index(drop=True)
        )
        return pair_scores, hand_scores
