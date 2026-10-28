package beakwecom

import (
	"context"
	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
	"regexp"
	"strings"
	"testing"
)

type cardGateway struct {
	testGateway
	actions []sdk.InteractionAction
}

type deniedCardGateway struct{ testGateway }
type requesterDenied struct{}

func (requesterDenied) Error() string                { return "denied" }
func (requesterDenied) InteractionErrorCode() string { return "actor_not_requester" }
func (*deniedCardGateway) HandleInteraction(context.Context, sdk.InteractionAction) (*sdk.InteractionResult, error) {
	return nil, requesterDenied{}
}

func TestWecomNonRequesterFeedbackTargetsOnlyClicker(t *testing.T) {
	account := testAccount()
	runtime := testRuntime(account, &deniedCardGateway{}, &testAccountStore{})
	body := map[string]any{"aibotid": "bot-1", "msgid": "event-1", "chatid": "group-1", "chattype": "group", "from": map[string]any{"userid": "other-member"}, "event": map[string]any{"eventtype": "template_card_event", "event_key": "approval_123:2:approve", "task_id": "approval_123_2"}}
	result, err := (Connector{}).HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: mustFrameData(t, platform.Frame{Command: platform.CommandEvent, Headers: platform.FrameHeaders{RequestID: "callback-1"}, Body: body})})
	if err != nil || len(result.ResponseFrames) != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if result.ResponseFrames[0].ACK == nil {
		t.Fatal("private feedback must observe the platform acknowledgement")
	}
	frame := mustParseFrame(t, result.ResponseFrames[0].Data)
	users, ok := frame.Body["userids"].([]any)
	if !ok || len(users) != 1 || users[0] != "other-member" {
		t.Fatalf("unexpected target: %#v", frame.Body["userids"])
	}
	card, ok := frame.Body["template_card"].(map[string]any)
	if !ok || card["task_id"] != "approval_123_2" || !strings.Contains(card["main_title"].(map[string]any)["desc"].(string), "发起人") {
		t.Fatalf("unexpected feedback: %#v", card)
	}
}

func (g *cardGateway) HandleInteraction(_ context.Context, a sdk.InteractionAction) (*sdk.InteractionResult, error) {
	g.actions = append(g.actions, a)
	return &sdk.InteractionResult{State: "approved", Card: &sdk.InteractionCard{RequestID: a.RequestID, Revision: a.Revision, Kind: "tool_approval", State: "approved", Title: "已批准", WebURL: "https://qa.example.test/agents/a/workspace"}}, nil
}
func TestWecomApprovalCallbackAndSupplementalStatus(t *testing.T) {
	account := testAccount()
	gateway := &cardGateway{}
	runtime := testRuntime(account, gateway, &testAccountStore{})
	stream := &testStreamTransport{}
	runtime.Stream = stream
	card := &sdk.InteractionCard{RequestID: "approval_123", Revision: 2, Kind: "tool_approval", State: "pending", Title: "确认", WebURL: "https://qa.example.test/agents/a/workspace", Actions: []sdk.InteractionButton{{ID: "approve", Label: "批准"}}}
	sent, err := (Connector{}).Send(context.Background(), runtime, sdk.OutboundMessage{AccountUUID: account.UUID, ChatID: "group-1", Interaction: card})
	if err != nil {
		t.Fatal(err)
	}
	frame := mustParseFrame(t, stream.requests[0].Frame.Data)
	if frame.Body["msgtype"] != "template_card" || sent.MessageID == "" {
		t.Fatalf("not a card: %+v", frame)
	}
	// Official callback buttons use text/key/style; type=1 selects a URL
	// button in the platform parser and is rejected without a URL (42028).
	buttons := frame.Body["template_card"].(map[string]any)["button_list"].([]any)
	button := buttons[0].(map[string]any)
	if _, exists := button["type"]; exists {
		t.Fatal("callback button must not be sent as a URL button")
	}
	if button["text"] != "批准" || button["key"] == "" {
		t.Fatal("callback button is missing its label or event key")
	}
	token, _ := card.ButtonToken(card.Actions[0])
	body := map[string]any{"aibotid": "bot-1", "msgid": "event-1", "chatid": "group-1", "chattype": "group", "from": map[string]any{"userid": "requester"}, "event": map[string]any{"eventtype": "template_card_event", "event_key": token, "task_id": "approval_123_2"}}
	callback := func() (*sdk.StreamFrameResult, error) {
		return (Connector{}).HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: mustFrameData(t, platform.Frame{Command: platform.CommandEvent, Headers: platform.FrameHeaders{RequestID: "callback-1"}, Body: body})})
	}
	result, err := callback()
	if err != nil {
		t.Fatal(err)
	}
	if len(gateway.actions) != 1 || gateway.actions[0].ActorID != "requester" || len(result.ResponseFrames) != 1 {
		t.Fatalf("missing authenticated action/update: %+v", result)
	}
	updated := mustParseFrame(t, result.ResponseFrames[0].Data)
	if updated.Command != "aibot_respond_update_msg" || updated.Headers.RequestID != "callback-1" {
		t.Fatalf("update=%+v", updated)
	}
	body["aibotid"] = "other-bot"
	_, err = callback()
	if err == nil || len(gateway.actions) != 1 {
		t.Fatal("foreign bot reached host")
	}
	card.State = "canceled"
	card.Actions = nil
	card.MessageID = sent.MessageID
	if _, err := (Connector{}).Send(context.Background(), runtime, sdk.OutboundMessage{AccountUUID: account.UUID, ChatID: "group-1", Interaction: card}); err != nil {
		t.Fatal(err)
	}
	last := mustParseFrame(t, stream.requests[len(stream.requests)-1].Frame.Data)
	if last.Command != platform.CommandSendMessage {
		t.Fatal("terminal outside callback must send supplemental status")
	}
	for _, statusFrame := range []platform.Frame{updated, last} {
		payload := statusFrame.Body["template_card"].(map[string]any)
		if payload["card_type"] != "button_interaction" {
			t.Fatal("status update must retain the button card type without requiring navigation")
		}
		if payload["replace_text"] == nil || payload["button_list"] == nil {
			t.Fatal("terminal card must replace its native button with a disabled status")
		}
		for _, forbidden := range []string{"card_action", "jump_list"} {
			if _, exists := payload[forbidden]; exists {
				t.Fatalf("status card contains %s; omit navigation entirely, including card_action.type=0", forbidden)
			}
		}
	}
	originalTask := frame.Body["template_card"].(map[string]any)["task_id"]
	if updated.Body["template_card"].(map[string]any)["task_id"] != originalTask {
		t.Fatal("callback update must retain the original task identity")
	}
	if last.Body["template_card"].(map[string]any)["task_id"] == originalTask {
		t.Fatal("new terminal notification must not reuse an already sent task ID")
	}
}

func TestWecomTaskIDUsesPlatformAlphabet(t *testing.T) {
	card := sdk.InteractionCard{RequestID: "approval_123", Revision: 35, Kind: "tool_approval", State: "pending", Title: "审批", WebURL: "https://qa.example.test/agents/a/workspace"}
	payload, err := wecomInteractionCard(card)
	if err != nil {
		t.Fatal(err)
	}
	taskID := payload["task_id"].(string)
	if len(taskID) > 128 || !regexp.MustCompile(`^[A-Za-z0-9_@-]+$`).MatchString(taskID) {
		t.Fatalf("invalid platform task ID %q", taskID)
	}
}

func TestWecomPlanDecisionPresentation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		state  string
		detail *sdk.PlanConfirmationDetails
		want   string
	}{
		{"pending", "pending", nil, "待确认"},
		{"approved", "approved", nil, "已批准"},
		{"rejected", "rejected", nil, "已拒绝"},
		{"revision waiting", "rejected", &sdk.PlanConfirmationDetails{DecisionAction: "revise", AwaitingRevisionInput: true}, "待补充修改意见"},
		{"revision supplied", "rejected", &sdk.PlanConfirmationDetails{DecisionAction: "revise"}, "已要求修改"},
		{"canceled", "canceled", nil, "已取消"},
		{"expired", "expired", nil, "已过期"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			card := sdk.InteractionCard{RequestID: "plan_123", Revision: 1, Kind: "plan_confirmation", State: tt.state, Title: "计划", Summary: "核对测试文字", PlanConfirmation: tt.detail, WebURL: "https://qa.example.test/agents/a/workspace"}
			payload, err := wecomInteractionCard(card)
			if err != nil {
				t.Fatal(err)
			}
			if payload["card_type"] != "button_interaction" {
				t.Fatal("cards without decisions must use a replaced status button")
			}
			if got := payload["main_title"].(map[string]string)["desc"]; got != tt.want {
				t.Fatalf("status description = %q, want %q", got, tt.want)
			}
			if payload["replace_text"] != tt.want {
				t.Fatal("terminal card must replace its button with its structured status")
			}
		})
	}
}

// The native callback nests button fields under template_card_event. Older
// published SDK examples showed flat fields; both explicit wire shapes exist.
func TestWecomNestedTemplateCardCallback(t *testing.T) {
	for _, actionID := range []string{"approve", "reject", "revise"} {
		t.Run(actionID, func(t *testing.T) {
			account := testAccount()
			gateway := &cardGateway{}
			runtime := testRuntime(account, gateway, &testAccountStore{})
			action := sdk.InteractionAction{RequestID: "plan_123", Revision: 2, ActionID: actionID}
			token, _ := action.Token()
			body := map[string]any{"aibotid": "bot-1", "msgid": "event-nested", "chattype": "single", "from": map[string]any{"userid": "requester"}, "event": map[string]any{"eventtype": "template_card_event", "template_card_event": map[string]any{"event_key": token, "task_id": "plan_123_2"}}}
			result, err := (Connector{}).HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: mustFrameData(t, platform.Frame{Command: platform.CommandEvent, Headers: platform.FrameHeaders{RequestID: "nested-callback"}, Body: body})})
			if err != nil {
				t.Fatal(err)
			}
			if len(gateway.actions) != 1 || gateway.actions[0].ActionID != actionID || gateway.actions[0].ActorID != "requester" || len(result.ResponseFrames) != 1 {
				t.Fatalf("missing nested callback: %+v", result)
			}
		})
	}
}

func TestWecomCardActionRejectsConflictingWireFields(t *testing.T) {
	event := platform.MessageEvent{EventKey: "old", TaskID: "task", TemplateCardEvent: &platform.TemplateCardEvent{EventKey: "new", TaskID: "task"}}
	if _, err := event.CardAction(); err == nil {
		t.Fatal("conflicting action fields accepted")
	}
	event.EventKey = "new"
	event.TaskID = "other"
	if _, err := event.CardAction(); err == nil {
		t.Fatal("conflicting task identity accepted")
	}
	event.EventKey = ""
	event.TaskID = ""
	event.TemplateCardEvent.EventKey = ""
	card, err := event.CardAction()
	if err != nil || card.EventKey != "" {
		t.Fatal("nested empty payload must not fall back to other fields")
	}
}

func TestWecomCardsNeverNavigateToWeb(t *testing.T) {
	for _, state := range []string{"pending", "approved", "rejected", "canceled", "expired"} {
		for _, webURL := range []string{"", "https://qa.example.test/agents/a/workspace?session_uuid=s"} {
			card := sdk.InteractionCard{RequestID: "approval_123", Revision: 1, Kind: "tool_approval", State: state, Title: "操作审批", WebURL: webURL}
			if state == "pending" {
				card.Actions = []sdk.InteractionButton{{ID: "approve", Label: "批准"}, {ID: "reject", Label: "拒绝"}}
			}
			payload, err := wecomInteractionCard(card)
			if err != nil {
				t.Fatal(err)
			}
			action, ok := payload["card_action"].(map[string]any)
			if (ok && (action["type"] != 0 || action["url"] != nil)) || payload["jump_list"] != nil {
				t.Fatalf("%s contains navigation: %+v", state, payload)
			}
		}
	}
}

func TestWecomRejectedResponsePreservesConnectionAndReportsFailure(t *testing.T) {
	account := testAccount()
	result, err := (Connector{}).HandleStreamFrame(context.Background(), testRuntime(account, &cardGateway{}, &testAccountStore{}), account, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: mustFrameData(t, platform.Frame{Headers: platform.FrameHeaders{RequestID: "callback-1"}, ErrCode: 42045, ErrMsg: "card_action Missing or Invalid"})})
	if err != nil || result == nil || result.ResponseTo != "callback-1" || result.Terminal || result.CloseReason != "" {
		t.Fatalf("message rejection must not close the host stream: %+v %v", result, err)
	}
	if !strings.Contains(stringValue(result.HealthUpdates[sdk.RuntimeHealthKeyStreamLastError]), "42045") {
		t.Fatalf("platform rejection missing from health diagnostics: %+v", result)
	}
}

func TestWecomToolApprovalPresentationKeepsRedactedDetailsAndDecisionIdentity(t *testing.T) {
	for _, state := range []string{"pending", "approved", "rejected", "canceled", "expired"} {
		card := sdk.InteractionCard{RequestID: "approval_style", Revision: 2, Kind: "tool_approval", State: state,
			Title: "操作审批", WebURL: "https://qa.example.test/agents/a/workspace",
			ToolApproval: &sdk.ToolApprovalDetails{ToolName: "exec", RiskLevel: "dangerous", ArgumentsPreview: "{\"command\":\"printf TEST\",\"token\":\"[REDACTED]\"}"}}
		if state == "pending" {
			card.Actions = []sdk.InteractionButton{{ID: "approve", Label: "批准"}, {ID: "reject", Label: "拒绝"}}
		}
		payload, err := wecomInteractionCard(card)
		if err != nil {
			t.Fatal(err)
		}
		quote := payload["quote_area"].(map[string]any)
		if quote["quote_text"] != card.ToolApproval.ArgumentsPreview {
			t.Fatal("redacted execution details lost")
		}
		rows := payload["horizontal_content_list"].([]map[string]string)
		if rows[0]["value"] != "exec" || rows[1]["value"] != "高风险" {
			t.Fatalf("details: %v", rows)
		}
		if state == "pending" {
			buttons := payload["button_list"].([]map[string]any)
			if len(buttons) != 2 || buttons[0]["style"] == buttons[1]["style"] {
				t.Fatal("decision styles not distinct")
			}
			for i, b := range buttons {
				token, _ := card.ButtonToken(card.Actions[i])
				if b["key"] != token {
					t.Fatal("changed decision identity")
				}
			}
		} else if payload["replace_text"] != wecomInteractionStateLabel(card) {
			t.Fatal("terminal decision buttons were not replaced with status text")
		}
	}
	if got := wecomDisplayPreview("中文参数内容", 4); got != "中文参…" {
		t.Fatalf("invalid unicode truncation: %q", got)
	}
}
