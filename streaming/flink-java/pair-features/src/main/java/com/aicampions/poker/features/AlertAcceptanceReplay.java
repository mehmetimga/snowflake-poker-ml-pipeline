package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bounded acceptance replay for the business logic used by the Flink job.
 *
 * <p>This runner deliberately has no Kafka source or sink. It executes the
 * same pair-feature math and stateful rule transition used inside the Flink
 * operators, which makes it suitable for deterministic CI parity. Kafka,
 * checkpoint, and deployment evidence are measured by the separate SPCS
 * replay.
 */
public final class AlertAcceptanceReplay {
    private static final StatefulFoldRuleEngine.Config STATEFUL_RULE_CONFIG =
            new StatefulFoldRuleEngine.Config(
                    24L * 3_600_000L,
                    5,
                    3,
                    0.6,
                    120_000L,
                    48L * 3_600_000L);

    private AlertAcceptanceReplay() {}

    public static void main(String[] arguments) throws Exception {
        Map<String, String> args = parseArguments(arguments);
        Path input = requiredPath(args, "input");
        Path expected = requiredPath(args, "expected");
        Path output = requiredPath(args, "output");

        long started = System.nanoTime();
        List<JsonNode> actual = replay(readJsonLines(input));
        List<JsonNode> oracle = readJsonLines(expected);
        writeJsonLines(output, actual);
        assertParity(actual, oracle);
        long durationMs = (System.nanoTime() - started) / 1_000_000L;

        ObjectNode report = PairEventJson.MAPPER.createObjectNode();
        report.put("schema_version", 1);
        report.put("status", "passed");
        report.put("runtime", "java-flink-shared-core");
        report.put("execution_scope", "feature-and-stateful-rule-business-logic");
        report.put("player_context_events", readJsonLines(input).size());
        report.put("pair_feature_events", actual.size());
        report.put("exact_pair_feature_matches", actual.size());
        report.put("duration_ms", durationMs);
        report.put("output", output.toAbsolutePath().normalize().toString());
        System.out.println(PairEventJson.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(report));
    }

    static List<JsonNode> replay(List<JsonNode> input) {
        List<JsonNode> ordered = new ArrayList<>(input);
        for (JsonNode event : ordered) {
            PairEventJson.validateEnriched(PairEventJson.compact(event), 1);
        }
        ordered.sort(Comparator
                .comparing((JsonNode event) -> PairEventJson.parseInstant(
                        PairEventJson.requireText(event.path("payload"), "played_at")))
                .thenComparing(event -> PairEventJson.requireText(
                        event.path("payload"), "hand_id"))
                .thenComparing(event -> PairEventJson.requireText(
                        event.path("payload").path("player"), "player_id"))
                .thenComparing(event -> PairEventJson.requireText(event, "emitted_at"))
                .thenComparing(event -> PairEventJson.requireText(event, "event_id")));

        Map<String, ObjectNode> userStates = new HashMap<>();
        Map<String, Map<String, JsonNode>> hands = new LinkedHashMap<>();
        Map<String, ObjectNode> pairStates = new HashMap<>();
        Map<String, String> statefulRuleStates = new HashMap<>();
        List<JsonNode> output = new ArrayList<>();

        for (JsonNode event : ordered) {
            JsonNode payload = event.path("payload");
            String playerId = PairEventJson.requireText(payload.path("player"), "player_id");
            String handId = PairEventJson.requireText(payload, "hand_id");
            ObjectNode userState =
                    userStates.computeIfAbsent(playerId, ignored -> PairFeatureMath.emptyUserState());
            JsonNode augmented = PairEventJson.parse(PairEventJson.augmentUser(
                    PairEventJson.compact(event),
                    PairFeatureMath.userSnapshot(userState)));
            PairFeatureMath.updateUser(userState, event);

            Map<String, JsonNode> rows =
                    hands.computeIfAbsent(handId, ignored -> new HashMap<>());
            if (rows.putIfAbsent(playerId, augmented) != null) {
                throw new IllegalArgumentException(
                        "acceptance replay contains duplicate player-hand input: "
                                + handId + "/" + playerId);
            }
            int expectedPlayers = payload.path("num_players").asInt();
            if (rows.size() < expectedPlayers) {
                continue;
            }
            if (rows.size() != expectedPlayers) {
                throw new IllegalArgumentException(
                        "acceptance replay assembled an unexpected player count");
            }
            List<String> players = new ArrayList<>(rows.keySet());
            players.sort(String::compareTo);
            for (int left = 0; left < players.size(); left++) {
                for (int right = left + 1; right < players.size(); right++) {
                    String playerA = players.get(left);
                    String playerB = players.get(right);
                    String pairKey = playerA + ":" + playerB;
                    JsonNode eventA = rows.get(playerA);
                    JsonNode eventB = rows.get(playerB);
                    JsonNode payloadA = eventA.path("payload");
                    ObjectNode pairState = pairStates.computeIfAbsent(
                            pairKey, ignored -> PairFeatureMath.emptyPairState());
                    Instant playedAt = PairEventJson.parseInstant(
                            PairEventJson.requireText(payloadA, "played_at"));
                    JsonNode pairHistory = PairFeatureMath.pairSnapshot(
                            pairState,
                            PairEventJson.requireText(payloadA, "table_id"),
                            playedAt);
                    JsonNode observation = PairEventJson.parse(
                            PairEventJson.pairObservation(
                                    pairKey,
                                    1,
                                    PairEventJson.compact(eventA),
                                    PairEventJson.compact(eventB)));
                    JsonNode feature = PairEventJson.parse(
                            PairFeatureMath.buildPairFeatureEvent(
                                    observation, pairHistory));
                    PairFeatureMath.updatePair(pairState, eventA, eventB);

                    StatefulFoldRuleEngine.Evaluation evaluation =
                            StatefulFoldRuleEngine.evaluate(
                                    statefulRuleStates.get(pairKey),
                                    feature,
                                    Long.MIN_VALUE,
                                    STATEFUL_RULE_CONFIG);
                    statefulRuleStates.put(pairKey, evaluation.stateJson());
                    output.add(StatefulFoldRuleEngine.enrichPairEvent(
                            feature, evaluation.evidenceEvent()));
                }
            }
        }
        return output;
    }

    static void assertParity(List<JsonNode> actual, List<JsonNode> expected) {
        if (actual.size() != expected.size()) {
            throw new IllegalStateException(
                    "pair-feature count mismatch: actual=" + actual.size()
                            + " expected=" + expected.size());
        }
        for (int index = 0; index < expected.size(); index++) {
            JsonNode actualEvent = actual.get(index);
            JsonNode expectedEvent = expected.get(index);
            if (!semanticEquals(actualEvent, expectedEvent)) {
                throw new IllegalStateException(
                        "pair-feature parity mismatch at index " + index
                                + ": actual_event_id="
                                + actualEvent.path("event_id").asText()
                                + " expected_event_id="
                                + expectedEvent.path("event_id").asText()
                                + " actual="
                                + PairEventJson.compact(actualEvent)
                                + " expected="
                                + PairEventJson.compact(expectedEvent));
            }
        }
    }

    private static boolean semanticEquals(JsonNode left, JsonNode right) {
        if (left.isNumber() && right.isNumber()) {
            return left.decimalValue().compareTo(right.decimalValue()) == 0;
        }
        if (left.isObject() && right.isObject()) {
            Set<String> leftFields = new HashSet<>();
            left.fieldNames().forEachRemaining(leftFields::add);
            Set<String> rightFields = new HashSet<>();
            right.fieldNames().forEachRemaining(rightFields::add);
            if (!leftFields.equals(rightFields)) {
                return false;
            }
            for (String field : leftFields) {
                if (!semanticEquals(left.path(field), right.path(field))) {
                    return false;
                }
            }
            return true;
        }
        if (left.isArray() && right.isArray()) {
            if (left.size() != right.size()) {
                return false;
            }
            for (int index = 0; index < left.size(); index++) {
                if (!semanticEquals(left.get(index), right.get(index))) {
                    return false;
                }
            }
            return true;
        }
        return left.equals(right);
    }

    private static List<JsonNode> readJsonLines(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("JSONL input does not exist: " + path);
        }
        List<JsonNode> output = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    output.add(PairEventJson.parse(line));
                }
            }
        }
        return output;
    }

    private static void writeJsonLines(Path path, List<JsonNode> values)
            throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (BufferedWriter writer =
                Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            for (JsonNode value : values) {
                writer.write(PairEventJson.compact(value));
                writer.newLine();
            }
        }
    }

    private static Map<String, String> parseArguments(String[] arguments) {
        Map<String, String> parsed = new HashMap<>();
        for (int index = 0; index < arguments.length; index++) {
            String argument = arguments[index];
            if (!argument.startsWith("--")
                    || index + 1 >= arguments.length
                    || arguments[index + 1].startsWith("--")) {
                throw new IllegalArgumentException(
                        "arguments must be --name value pairs");
            }
            String name = argument.substring(2);
            if (parsed.put(name, arguments[++index]) != null) {
                throw new IllegalArgumentException("duplicate argument --" + name);
            }
        }
        return parsed;
    }

    private static Path requiredPath(Map<String, String> arguments, String name) {
        String value = arguments.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--" + name + " is required");
        }
        return Path.of(value).toAbsolutePath().normalize();
    }
}
