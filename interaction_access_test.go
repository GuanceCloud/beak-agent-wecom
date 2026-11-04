package beakwecom

import (
	"context"
	"errors"
	"testing"

	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

type snapshotDenial struct {
	requesterDenied
	card *sdk.InteractionCard
}

func (e snapshotDenial) InteractionCard() *sdk.InteractionCard { return e.card }

type deniedCallbackGateway struct {
	testGateway
	err       error
	completed chan bool
}

func (g *deniedCallbackGateway) BeginCallbackInteraction(context.Context, sdk.InteractionAction) (*sdk.InteractionResult, func(bool), error) {
	return nil, func(ok bool) { g.completed <- ok }, g.err
}

func privateFeedback(t *testing.T, gateway *deniedCallbackGateway, action string) (*sdk.StreamFrameResult, error) {
	t.Helper()
	account := testAccount()
	body := map[string]any{"aibotid": "bot-1", "msgid": "event-1", "chatid": "group-1", "chattype": "group",
		"from": map[string]any{"userid": "B"}, "event": map[string]any{"eventtype": "template_card_event",
			"event_key": "approval_123:2:" + action, "task_id": "approval_123_2"}}
	return (Connector{}).HandleStreamFrame(context.Background(), testRuntime(account, gateway, &testAccountStore{}), account,
		sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: mustFrameData(t, platform.Frame{
			Command: platform.CommandEvent, Headers: platform.FrameHeaders{RequestID: "callback-1"}, Body: body})})
}

func TestPrivateFeedbackPreservesTerminalStateAndCompletesLease(t *testing.T) {
	for _, action := range []string{"approve", "reject"} {
		for _, state := range []string{"pending", "approved", "rejected"} {
			for _, ack := range []string{"success", "rejected", "missing"} {
				t.Run(action+"/"+state+"/"+ack, func(t *testing.T) {
					card := (requesterDenied{}).InteractionCard()
					card.State, card.Language = state, "en"
					if state != "pending" {
						card.Actions = nil
					}
					gateway := &deniedCallbackGateway{err: snapshotDenial{card: card}, completed: make(chan bool, 2)}
					result, err := privateFeedback(t, gateway, action)
					if err != nil || len(result.ResponseFrames) != 1 {
						t.Fatalf("result=%+v err=%v", result, err)
					}
					frame := mustParseFrame(t, result.ResponseFrames[0].Data)
					payload := frame.Body["template_card"].(map[string]any)
					want := wecomInteractionStateLabel(*card)
					if state == "pending" {
						want = "Approval unavailable. You do not have permission to approve."
					}
					if payload["replace_text"] != want {
						t.Fatalf("state overwritten: %v", payload)
					}
					select {
					case <-gateway.completed:
						t.Fatal("lease released before ACK")
					default:
					}
					callback := result.ResponseFrames[0].ACK
					if callback == nil {
						t.Fatal("missing ACK")
					}
					if ack == "missing" {
						callback.Complete(nil, errors.New("timeout"))
					} else {
						code := 0
						if ack == "rejected" {
							code = 42045
						}
						callback.Complete(&sdk.StreamResponse{Frame: sdk.StreamFrame{Data: mustFrameData(t, platform.Frame{Headers: platform.FrameHeaders{RequestID: "callback-1"}, ErrCode: code})}}, nil)
					}
					if got := <-gateway.completed; got != (ack == "success") {
						t.Fatalf("ACK completion=%v", got)
					}
					if card.State != state {
						t.Fatal("host snapshot mutated")
					}
				})
			}
		}
	}
}

func TestPrivateFeedbackWithoutMatchingSnapshotLeavesCardIntact(t *testing.T) {
	wrong := (requesterDenied{}).InteractionCard()
	wrong.RequestID = "another_approval"
	for _, card := range []*sdk.InteractionCard{nil, wrong} {
		gateway := &deniedCallbackGateway{err: snapshotDenial{card: card}, completed: make(chan bool, 2)}
		result, err := privateFeedback(t, gateway, "reject")
		if err == nil || result != nil && len(result.ResponseFrames) != 0 {
			t.Fatal("incomplete snapshot replaced the card")
		}
		if <-gateway.completed {
			t.Fatal("failed rendering reported successful delivery")
		}
	}
}
