package beakwecom

import (
	"context"
	"errors"
	"fmt"
	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
	"strconv"
	"strings"
	"time"
)

func wecomInteractionCard(card sdk.InteractionCard) (map[string]any, error) {
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
					label = "批准"
				case "revise":
					label = "修改"
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
	if err != nil {
		var access interface{ InteractionErrorCode() string }
		if errors.As(err, &access) && access.InteractionErrorCode() == "actor_not_requester" {
			// Only replace the clicking member's view. Other members keep their
			// original card and the host has not recorded an approval decision.
			card := map[string]any{
				"card_type":    "button_interaction",
				"task_id":      cardAction.TaskID,
				"main_title":   map[string]string{"title": "无法审批", "desc": "只有本次任务的发起人可以审批，请联系发起人处理。"},
				"button_list":  []map[string]any{{"text": "请联系发起人", "key": cardAction.EventKey, "style": 4}},
				"replace_text": "请联系发起人",
			}
			payload, marshalErr := platform.MarshalFrame(platform.Frame{Command: "aibot_respond_update_msg", Headers: frame.Headers, Body: map[string]any{
				"response_type": "update_template_card", "userids": []string{action.ActorID}, "template_card": card,
			}})
			if marshalErr != nil {
				return out, marshalErr
			}
			response := sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload}
			response.ACK = &sdk.StreamFrameACK{
				CorrelationID: frame.Headers.RequestID,
				Timeout:       streamRequestTimeout,
				Complete: func(response *sdk.StreamResponse, ackErr error) {
					if runtime.Logger == nil {
						return
					}
					if ackErr != nil || response == nil {
						runtime.Logger.Printf("wecom private approval feedback was not acknowledged")
						return
					}
					ack, parseErr := platform.ParseFrame(response.Frame.Data)
					if parseErr != nil || ack.ErrCode != 0 {
						runtime.Logger.Printf("wecom private approval feedback was rejected")
					}
				},
			}
			out.ResponseFrames = []sdk.StreamFrame{response}
			out.EventResult = &sdk.StreamEventResult{Type: "interaction"}
			return out, nil
		}
		return out, err
	}
	installed := false
	defer func() {
		if !installed && complete != nil {
			complete(false)
		}
	}()
	if result != nil && result.Card != nil {
		card, err := wecomInteractionCard(*result.Card)
		if err != nil {
			return out, err
		}
		payload, err := platform.MarshalFrame(platform.Frame{Command: "aibot_respond_update_msg", Headers: frame.Headers, Body: map[string]any{"response_type": "update_template_card", "template_card": card}})
		if err != nil {
			return out, err
		}
		response := sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload}
		if complete != nil {
			response.ACK = &sdk.StreamFrameACK{
				CorrelationID: frame.Headers.RequestID,
				Timeout:       streamRequestTimeout,
				Complete: func(response *sdk.StreamResponse, ackErr error) {
					if ackErr != nil || response == nil {
						complete(false)
						return
					}
					ack, parseErr := platform.ParseFrame(response.Frame.Data)
					complete(parseErr == nil && ack.ErrCode == 0)
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
	if detail := card.PlanConfirmation; detail != nil && card.State == "rejected" && detail.DecisionAction == "revise" {
		if detail.AwaitingRevisionInput {
			return "待补充修改意见"
		}
		return "已要求修改"
	}
	switch card.State {
	case "approved":
		return "已批准"
	case "rejected":
		return "已拒绝"
	case "canceled":
		return "已取消"
	case "expired":
		return "已过期"
	default:
		if card.Kind == "tool_approval" {
			return "待审批"
		}
		return "待确认"
	}
}

// The platform renders native fields, not Markdown. Use only host-redacted display
// data; never infer tool identity, authorization, or lifecycle from summary text.
func styleWecomToolApproval(value map[string]any, card sdk.InteractionCard) {
	color := 1
	hint := "请确认执行内容，批准后才会继续"
	switch card.State {
	case "approved":
		color, hint = 3, "已允许执行，实际结果请查看后续回复"
	case "rejected":
		color, hint = 2, "已拒绝本次操作，不会执行"
	case "canceled":
		color, hint = 0, "本次审批已取消，请以最新请求为准"
	case "expired":
		color, hint = 0, "本次审批已过期，请重新发起请求"
	}
	if card.ToolApproval != nil && card.ToolApproval.ResolutionText != "" && (card.State == "canceled" || card.State == "expired") {
		hint = card.ToolApproval.ResolutionText
	}
	value["source"] = map[string]any{"desc": "操作审批 · " + wecomInteractionStateLabel(card), "desc_color": color}
	value["main_title"] = map[string]string{"title": "确认工具操作", "desc": hint}
	detail := card.ToolApproval
	if detail == nil {
		value["sub_title_text"] = wecomDisplayPreview(card.Summary, 112)
		return
	}
	risk := "需确认"
	switch detail.RiskLevel {
	case "dangerous", "high":
		risk = "高风险"
	case "medium":
		risk = "中风险"
	case "low", "safe":
		risk = "低风险"
	}
	value["horizontal_content_list"] = []map[string]string{
		{"keyname": "执行工具", "value": wecomDisplayPreview(detail.ToolName, 26)},
		{"keyname": "风险等级", "value": risk},
	}
	preview := strings.TrimSpace(detail.ArgumentsPreview)
	if preview == "" {
		preview = "未提供参数预览，请先向发起人确认执行内容。"
	}
	title := "执行内容（已脱敏）"
	if detail.ArgumentsTruncated || len([]rune(preview)) > 280 {
		title = "执行内容（摘要）"
	}
	value["quote_area"] = map[string]any{"type": 0, "title": title, "quote_text": wecomDisplayPreview(preview, 280)}
	value["sub_title_text"] = "仅对本次请求作出决定，敏感参数已隐藏。"
}

func wecomDisplayPreview(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit-1]) + "…"
}
