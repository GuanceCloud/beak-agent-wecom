package beakwecom

import (
	"context"
	"errors"
	"fmt"
	"github.com/GuanceCloud/beak-agent-wecom/internal/locale"
	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
	"strconv"
	"strings"
	"time"
)

// interactionAccessFeedback is an optional authenticated host error contract.
// The snapshot must belong to the verified account, conversation and request.
// It is display-only; private feedback is not an approval decision. When using
// CallbackInteractionGateway, the host holds its delivery lease until Complete
// is called, including when returning this error. Missing snapshots leave the
// original card unchanged.
type interactionAccessFeedback interface {
	error
	InteractionErrorCode() string
	InteractionCard() *sdk.InteractionCard
	InteractionRequesterName() string
}

func wecomInteractionCard(card sdk.InteractionCard) (map[string]any, error) {
	tr := locale.Translator(sdk.ResolveInteractionLanguage("wecom", card.Language))
	if err := card.Validate(); err != nil {
		return nil, err
	}
	if len(card.Inputs) > 0 {
		return nil, fmt.Errorf("wecom native input is unavailable")
	}
	value := map[string]any{"card_type": "button_interaction", "main_title": map[string]string{"title": card.Title, "desc": wecomInteractionStateLabel(card)}, "sub_title_text": card.Summary, "task_id": card.RequestID + "_" + strconv.FormatInt(card.Revision, 36)}
	if card.Kind == "tool_approval" {
		styleWecomToolApproval(value, card)
	}
	// Keep the non-navigating button card. text_notice requires card_action
	// on the live bot API (42045), and a button card without button_list is
	// rejected (40016). replace_text renders the button area as completed.
	if len(card.Actions) == 0 {
		label := wecomInteractionStateLabel(card)
		actionID := "reject"
		if card.State == "approved" {
			actionID = "approve"
		}
		token, err := card.ButtonToken(sdk.InteractionButton{ID: actionID})
		if err != nil {
			return nil, err
		}
		value["button_list"] = []map[string]any{{"text": label, "key": token, "style": 4}}
		value["replace_text"] = label
	}
	if len(card.Actions) > 0 {
		buttons := make([]map[string]any, 0, len(card.Actions))
		for _, button := range card.Actions {
			token, err := card.ButtonToken(button)
			if err != nil {
				return nil, err
			}
			label := button.Label
			if card.Kind == "plan_confirmation" {
				switch button.ID {
				case "approve":
					label = tr("Approve")
				case "revise":
					label = tr("Revise")
				}
			}
			style := 1
			if button.ID == "reject" {
				style = 2
			}
			buttons = append(buttons, map[string]any{"text": label, "key": token, "style": style})
		}
		value["button_list"] = buttons
	}
	return value, nil
}
func sendWecomInteraction(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	if runtime.Stream == nil {
		return nil, fmt.Errorf("wecom host stream transport is unavailable")
	}
	card, err := wecomInteractionCard(*req.Interaction)
	if err != nil {
		return nil, err
	}
	requestID, err := platform.NewRequestID(platform.CommandSendMessage)
	if err != nil {
		return nil, err
	}
	if len(req.Interaction.Actions) == 0 {
		// A supplemental notification is a new platform message. Unlike a
		// callback update, it must not reuse the pending card's task ID.
		card["task_id"] = requestID
	}
	payload, err := platform.MarshalFrame(platform.Frame{Command: platform.CommandSendMessage, Headers: platform.FrameHeaders{RequestID: requestID}, Body: map[string]any{"chatid": req.ChatID, "msgtype": "template_card", "template_card": card}})
	if err != nil {
		return nil, err
	}
	// Proactive updates are unavailable. A terminal notification replaces its
	// buttons with status text; the host also enforces the terminal decision.
	response, err := runtime.Stream.Request(ctx, sdk.StreamRequest{Frame: sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload}, CorrelationID: requestID, Timeout: streamRequestTimeout})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("wecom card response missing")
	}
	ack, err := platform.ParseFrame(response.Frame.Data)
	if err != nil {
		return nil, err
	}
	if ack.ErrCode != 0 {
		return nil, fmt.Errorf("wecom card send failed: code=%d", ack.ErrCode)
	}
	return &sdk.SendResult{Platform: Platform, AccountUUID: req.AccountUUID, MessageID: firstString(ack.Body["msgid"], requestID)}, nil
}
func handleWecomInteraction(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, frame platform.Frame, message platform.Message, out *sdk.StreamFrameResult) (*sdk.StreamFrameResult, error) {
	if message.BotID == "" || message.BotID != stringValue(account.Credential["bot_id"]) {
		return out, fmt.Errorf("wecom card bot identity mismatch")
	}
	cardAction, err := message.Event.CardAction()
	if err != nil {

		return out, err
	}
	action, err := sdk.ParseInteractionToken(cardAction.EventKey)
	if err != nil {
		return out, err
	}
	if cardAction.TaskID != action.RequestID+"_"+strconv.FormatInt(action.Revision, 36) {
		return out, fmt.Errorf("wecom card task identity mismatch")
	}
	_, action.ChatID = wecomChat(message)
	action.ActorID = message.From.UserID
	action.EventID = message.MessageID
	if action.ActorID == "" || action.ChatID == "" || action.EventID == "" {
		return out, fmt.Errorf("interaction platform identity missing")
	}
	callbackCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var result *sdk.InteractionResult
	var complete func(bool)
	if coordinator, ok := runtime.Gateway.(sdk.CallbackInteractionGateway); ok {
		result, complete, err = coordinator.BeginCallbackInteraction(callbackCtx, action)
	} else {
		result, err = sdk.DispatchInteraction(callbackCtx, runtime, action)
	}
	installed := false
	defer func() {
		if !installed && complete != nil {
			complete(false)
		}
	}()
	var denied interactionAccessFeedback
	if err != nil {
		// A trusted host snapshot is required: never erase the original evidence
		// or reconstruct it from the callback's untrusted display fields.
		if !errors.As(err, &denied) || denied.InteractionErrorCode() != "actor_not_requester" {
			return out, err
		}
		card := denied.InteractionCard()
		if card == nil || card.RequestID != action.RequestID || card.Revision != action.Revision ||
			strings.TrimSpace(denied.InteractionRequesterName()) == "" {
			return out, err
		}
		result = &sdk.InteractionResult{State: card.State, Card: card}
	}
	if result != nil && result.Card != nil {
		card, err := wecomInteractionCard(*result.Card)
		if err != nil {
			return out, err
		}
		body := map[string]any{"response_type": "update_template_card", "template_card": card}
		if denied != nil {
			body["userids"] = []string{action.ActorID}
			if result.Card.State == "pending" {
				tr := locale.Translator(sdk.ResolveInteractionLanguage("wecom", result.Card.Language))
				label := fmt.Sprintf(tr("Approval unavailable. Only %s can decide this approval."), denied.InteractionRequesterName())
				card["button_list"] = []map[string]any{{"text": tr("Approval unavailable"), "key": cardAction.EventKey, "style": 4}}
				card["replace_text"] = label
			}
		}
		payload, err := platform.MarshalFrame(platform.Frame{Command: "aibot_respond_update_msg", Headers: frame.Headers, Body: body})
		if err != nil {
			return out, err
		}
		response := sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload}
		if complete != nil || denied != nil {
			response.ACK = &sdk.StreamFrameACK{
				CorrelationID: frame.Headers.RequestID,
				Timeout:       streamRequestTimeout,
				Complete: func(response *sdk.StreamResponse, ackErr error) {
					success := false
					if ackErr == nil && response != nil {
						ack, parseErr := platform.ParseFrame(response.Frame.Data)
						success = parseErr == nil && ack.ErrCode == 0
					}
					if complete != nil {
						complete(success)
					}
					if !success && runtime.Logger != nil {
						runtime.Logger.Printf("wecom approval update was not acknowledged or was rejected")
					}
				},
			}
			installed = true
		}
		out.ResponseFrames = []sdk.StreamFrame{response}
	}
	out.EventResult = &sdk.StreamEventResult{Type: "interaction"}
	return out, nil
}

func wecomInteractionStateLabel(card sdk.InteractionCard) string {
	tr := locale.Translator(sdk.ResolveInteractionLanguage("wecom", card.Language))
	if detail := card.PlanConfirmation; detail != nil && card.State == "rejected" && detail.DecisionAction == "revise" {
		if detail.AwaitingRevisionInput {
			return tr("Awaiting change details")
		}
		return tr("Changes requested")
	}
	switch card.State {
	case "approved":
		return tr("Approved")
	case "rejected":
		return tr("Rejected")
	case "canceled":
		return tr("Canceled")
	case "expired":
		return tr("Expired")
	default:
		if card.Kind == "tool_approval" {
			return tr("Awaiting approval")
		}
		return tr("Awaiting confirmation")
	}
}

// The platform renders native fields, not Markdown. Use only host-redacted display
// data; never infer tool identity, authorization, or lifecycle from summary text.
func styleWecomToolApproval(value map[string]any, card sdk.InteractionCard) {
	tr := locale.Translator(sdk.ResolveInteractionLanguage("wecom", card.Language))
	color := 1
	hint := tr("Review the operation. Execution continues only after approval.")
	switch card.State {
	case "approved":
		color, hint = 3, tr("Execution authorized. See the following reply for results.")
	case "rejected":
		color, hint = 2, tr("Operation rejected. It will not execute.")
	case "canceled":
		color, hint = 0, tr("Approval canceled. Refer to the latest request.")
	case "expired":
		color, hint = 0, tr("Approval expired. Send a new request.")
	}
	if card.ToolApproval != nil && card.ToolApproval.ResolutionText != "" && (card.State == "canceled" || card.State == "expired") {
		hint = card.ToolApproval.ResolutionText
	}
	value["source"] = map[string]any{"desc": tr("Tool approval · ") + wecomInteractionStateLabel(card), "desc_color": color}
	value["main_title"] = map[string]string{"title": tr("Confirm tool operation"), "desc": hint}
	detail := card.ToolApproval
	if detail == nil {
		value["sub_title_text"] = wecomDisplayPreview(card.Summary, 112)
		return
	}
	risk := tr("Review required")
	switch detail.RiskLevel {
	case "dangerous", "high":
		risk = tr("High risk")
	case "medium":
		risk = tr("Medium risk")
	case "low", "safe":
		risk = tr("Low risk")
	}
	value["horizontal_content_list"] = []map[string]string{
		{"keyname": tr("Tool"), "value": wecomDisplayPreview(detail.ToolName, 26)},
		{"keyname": tr("Risk level"), "value": risk},
	}
	preview := strings.TrimSpace(detail.ArgumentsPreview)
	if preview == "" {
		preview = tr("No argument preview provided. Confirm the operation with the requester first.")
	}
	// Keep the full host-redacted arguments in the sole approval card.
	// Clients may collapse the quote visually; do not truncate its payload.
	title := tr("Operation (preview)")
	value["quote_area"] = map[string]any{"type": 0, "title": title, "quote_text": preview}
	value["sub_title_text"] = tr("Request: ") + card.RequestID
}

func wecomDisplayPreview(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit-1]) + "…"
}
