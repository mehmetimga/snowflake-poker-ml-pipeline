package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class HandPairAssemblyFunction extends KeyedProcessFunction<String, String, String> {
    private final long stateTtlHours;
    private final String sourceTopic;
    private transient MapState<String, String> rowsByPlayer;
    private transient MapState<String, String> signaturesByPair;
    private transient MapState<String, Integer> revisionsByPair;

    HandPairAssemblyFunction(long stateTtlHours, String sourceTopic) {
        this.stateTtlHours = stateTtlHours;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(Configuration parameters) {
        rowsByPlayer = getRuntimeContext().getMapState(
                StateDescriptors.stringMap("pair-features-hand-players-v1", stateTtlHours));
        signaturesByPair = getRuntimeContext().getMapState(
                StateDescriptors.stringMap("pair-features-hand-signatures-v1", stateTtlHours));
        revisionsByPair = getRuntimeContext().getMapState(
                StateDescriptors.integerMap("pair-features-hand-revisions-v1", stateTtlHours));
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            JsonNode incoming = PairEventJson.parse(value);
            JsonNode payload = incoming.path("payload");
            String playerId = PairEventJson.requireText(payload.path("player"), "player_id");
            String previousJson = rowsByPlayer.get(playerId);
            if (previousJson != null) {
                JsonNode previous = PairEventJson.parse(previousJson);
                int incomingRevision = payload.path("revision").asInt();
                int previousRevision = previous.path("payload").path("revision").asInt();
                if (incomingRevision < previousRevision) {
                    return;
                }
                if (incomingRevision == previousRevision) {
                    if (!incoming.path("event_id").equals(previous.path("event_id"))) {
                        throw new IllegalArgumentException(
                                "same player-hand revision has conflicting event IDs");
                    }
                    return;
                }
            }
            rowsByPlayer.put(playerId, value);
            List<JsonNode> rows = new ArrayList<>();
            for (String row : rowsByPlayer.values()) {
                rows.add(PairEventJson.parse(row));
            }
            int expectedPlayers = payload.path("num_players").asInt();
            if (rows.size() < expectedPlayers) {
                return;
            }
            validateCompleteHand(rows, expectedPlayers);
            rows.sort(Comparator.comparing(
                    row -> PairEventJson.requireText(row.path("payload").path("player"), "player_id")));
            for (int left = 0; left < rows.size(); left++) {
                for (int right = left + 1; right < rows.size(); right++) {
                    JsonNode rowA = rows.get(left);
                    JsonNode rowB = rows.get(right);
                    String playerA = PairEventJson.requireText(
                            rowA.path("payload").path("player"), "player_id");
                    String playerB = PairEventJson.requireText(
                            rowB.path("payload").path("player"), "player_id");
                    String pairKey = playerA + ":" + playerB;
                    String signature = rowA.path("event_id").asText()
                            + ":" + rowB.path("event_id").asText();
                    if (signature.equals(signaturesByPair.get(pairKey))) {
                        continue;
                    }
                    signaturesByPair.put(pairKey, signature);
                    Integer previousRevision = revisionsByPair.get(pairKey);
                    int revision = previousRevision == null ? 1 : previousRevision + 1;
                    revisionsByPair.put(pairKey, revision);
                    output.collect(PairEventJson.pairObservation(
                            pairKey, revision, PairEventJson.compact(rowA), PairEventJson.compact(rowB)));
                }
            }
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(sourceTopic, "assemble-hand-pairs", error.getMessage(), value));
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    private static void validateCompleteHand(List<JsonNode> rows, int expectedPlayers) {
        if (rows.size() != expectedPlayers) {
            throw new IllegalArgumentException("assembled hand has an unexpected player count");
        }
        JsonNode first = rows.get(0);
        JsonNode firstPayload = first.path("payload");
        String expected = String.join(
                "|",
                first.path("dataset_id").asText(),
                first.path("dataset_split").asText(),
                firstPayload.path("source_hand_event_id").asText(),
                firstPayload.path("table_id").asText(),
                firstPayload.path("played_at").asText(),
                firstPayload.path("num_players").asText());
        for (JsonNode row : rows) {
            JsonNode payload = row.path("payload");
            String actual = String.join(
                    "|",
                    row.path("dataset_id").asText(),
                    row.path("dataset_split").asText(),
                    payload.path("source_hand_event_id").asText(),
                    payload.path("table_id").asText(),
                    payload.path("played_at").asText(),
                    payload.path("num_players").asText());
            if (!actual.equals(expected)) {
                throw new IllegalArgumentException("player rows disagree on hand metadata");
            }
        }
    }
}
