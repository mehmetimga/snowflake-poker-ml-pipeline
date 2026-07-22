package cdc

import (
	"encoding/json"
	"strings"
	"time"

	handpb "github.com/ai-campions/snowflake-poker-ml-pipeline/services/go/internal/cdc/proto"
	"google.golang.org/protobuf/proto"
)

const microsPerUnit = 1_000_000.0

// SimulationProtobufDecoder decodes only the repository-owned simulation
// format. It is not a poker-server binary decoder.
type SimulationProtobufDecoder struct{}

func (SimulationProtobufDecoder) Version() string { return SimulationProtobufCodec }

func (SimulationProtobufDecoder) Decode(
	payload []byte,
	context DecodeContext,
) (json.RawMessage, error) {
	message := &handpb.HandHistoryV1{}
	if err := proto.Unmarshal(payload, message); err != nil {
		return nil, reject("invalid_binary_payload", "simulation payload is not Protobuf v1")
	}
	if message.GameType != context.GameType {
		return nil, reject(
			"game_type_mismatch",
			"outbox game type %q != binary %q",
			context.GameType,
			message.GameType,
		)
	}
	hand := handPayload{
		HandID: message.HandId, TableID: message.TableId,
		PlayedAt:     time.UnixMilli(message.PlayedAtUnixMs).UTC().Format(time.RFC3339Nano),
		DatasetSplit: message.DatasetSplit, Generator: message.Generator,
		SmallBlind: float64(message.SmallBlindMicros) / microsPerUnit,
		BigBlind:   float64(message.BigBlindMicros) / microsPerUnit,
		NumPlayers: int(message.NumPlayers),
		PotSize:    float64(message.PotSizeMicros) / microsPerUnit,
		Board:      append([]string{}, message.Board...),
		Actions:    make([]handAction, 0, len(message.Actions)),
		Players:    make([]handPlayer, 0, len(message.Players)),
	}
	for _, action := range message.Actions {
		if action == nil {
			return nil, reject("invalid_binary_payload", "simulation action is null")
		}
		hand.Actions = append(hand.Actions, handAction{
			SequenceNo: int(action.SequenceNo), PlayerID: action.PlayerId,
			Street: action.Street, ActionType: action.ActionType,
			Amount: float64(action.AmountMicros) / microsPerUnit,
		})
	}
	for _, player := range message.Players {
		if player == nil {
			return nil, reject("invalid_binary_payload", "simulation player is null")
		}
		hand.Players = append(hand.Players, handPlayer{
			PlayerID: player.PlayerId, Name: player.Name, Position: player.Position,
			Stack:     float64(player.StackStartMicros) / microsPerUnit,
			HoleCards: strings.Join(player.HoleCards, " "),
			WonAmount: float64(player.WonAmountMicros) / microsPerUnit,
		})
	}
	encoded, err := json.Marshal(hand)
	if err != nil {
		return nil, reject("invalid_binary_payload", "cannot canonicalize simulation hand")
	}
	return encoded, nil
}
