package beakwecom

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

const (
	wecomPingInterval = 30 * time.Second
	wecomPongTimeout  = 10 * time.Second
)

type streamState struct {
	AuthRequestID string
	BotID         string
}

func (Connector) ConnectStream(_ context.Context, runtime sdk.Runtime, account sdk.ChannelAccount) (*sdk.StreamConnectResult, error) {
	botID := strings.TrimSpace(stringValue(account.Credential["bot_id"]))
	secret := strings.TrimSpace(stringValue(account.Credential["secret"]))
	if botID == "" {
		return nil, fmt.Errorf("wecom stream bot_id is required")
	}
	if secret == "" {
		return nil, fmt.Errorf("wecom stream secret is required")
	}
	requestID, err := platform.NewRequestID(platform.CommandSubscribe)
	if err != nil {
		return nil, err
	}
	payload, err := platform.MarshalFrame(platform.AuthFrame(requestID, botID, secret))
	if err != nil {
		return nil, err
	}
	endpoint := strings.TrimSpace(nativeRuntime(runtime.Native).WebSocketURL)
	if endpoint == "" {
		endpoint = platform.DefaultWebSocketURL
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return &sdk.StreamConnectResult{
		URL:             endpoint,
		ServiceID:       firstString(account.UUID, botID),
		ReadMessageType: sdk.StreamMessageTypeText,
		InitialFrames: []sdk.StreamFrame{{
			MessageType: sdk.StreamMessageTypeText,
			Data:        payload,
		}},
		WaitForReady: true,
		PingInterval: wecomPingInterval,
		PongTimeout:  wecomPongTimeout,
		State:        streamState{AuthRequestID: requestID, BotID: botID},
		HealthUpdates: map[string]any{
			sdk.RuntimeHealthKeyStreamConnectionState:      sdk.RuntimeHealthStateReconnecting,
			sdk.RuntimeHealthKeyStreamReconnectRequestedAt: now,
		},
	}, nil
}

func (Connector) BuildStreamPing(context.Context, sdk.StreamPingRequest) (*sdk.StreamFrame, error) {
	requestID, err := platform.NewRequestID(platform.CommandPing)
	if err != nil {
		return nil, err
	}
	payload, err := platform.MarshalFrame(platform.PingFrame(requestID))
	if err != nil {
		return nil, err
	}
	return &sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload}, nil
}

func (c Connector) HandleStreamFrame(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req sdk.StreamFrameRequest) (*sdk.StreamFrameResult, error) {
	out := &sdk.StreamFrameResult{State: req.State}
	if req.MessageType != 0 && req.MessageType != sdk.StreamMessageTypeText {
		return out, nil
	}
	frame, err := platform.ParseFrame(req.Data)
	if err != nil {
		return out, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	out.HealthUpdates = map[string]any{sdk.RuntimeHealthKeyStreamLastActivityAt: now}
	state := decodeStreamState(req.State)

	if frame.Command == "" {
		switch {
		case frame.Headers.RequestID == state.AuthRequestID || strings.HasPrefix(frame.Headers.RequestID, platform.CommandSubscribe+"_"):
			if frame.ErrCode != 0 {
				message := fmt.Sprintf("wecom stream authentication failed: code=%d message=%s", frame.ErrCode, strings.TrimSpace(frame.ErrMsg))
				out.CloseReason = message
				out.Terminal = true
				out.HealthUpdates = map[string]any{
					sdk.RuntimeHealthKeyStreamConnectionState:  sdk.RuntimeHealthStateReconnectFailed,
					sdk.RuntimeHealthKeyStreamDisconnectedAt:   now,
					sdk.RuntimeHealthKeyStreamLastError:        message,
					sdk.RuntimeHealthKeyStreamLastErrorAt:      now,
					sdk.RuntimeHealthKeyStreamReconnectError:   message,
					sdk.RuntimeHealthKeyStreamReconnectErrorAt: now,
				}
				return out, nil
			}
			out.Ready = true
			out.HealthUpdates = map[string]any{
				sdk.RuntimeHealthKeyStreamConnectionState:  sdk.RuntimeHealthStateConnected,
				sdk.RuntimeHealthKeyStreamConnectedAt:      now,
				sdk.RuntimeHealthKeyStreamLastActivityAt:   now,
				sdk.RuntimeHealthKeyStreamLastError:        "",
				sdk.RuntimeHealthKeyStreamLastErrorAt:      "",
				sdk.RuntimeHealthKeyStreamReconnectError:   "",
				sdk.RuntimeHealthKeyStreamReconnectErrorAt: "",
			}
			return out, nil
		case strings.HasPrefix(frame.Headers.RequestID, platform.CommandPing+"_"):
			if frame.ErrCode != 0 {
				return out, fmt.Errorf("wecom stream heartbeat failed: code=%d message=%s", frame.ErrCode, strings.TrimSpace(frame.ErrMsg))
			}
			out.HealthUpdates[sdk.RuntimeHealthKeyStreamLastPongAt] = now
			return out, nil
		default:
			out.ResponseTo = frame.Headers.RequestID
			return out, nil
		}
	}

	switch frame.Command {
	case platform.CommandMessage:
		result, err := c.processMessageFrame(ctx, runtime, account, frame)
		out.EventResult = streamEventResult(result)
		if result != nil && !result.Ignored && err == nil {
			out.HealthUpdates[sdk.RuntimeHealthKeyStreamLastEventAt] = now
		}
		return out, err
	case platform.CommandEvent:
		message, err := platform.DecodeMessage(frame.Body)
		if err != nil {
			return out, err
		}
		eventType := strings.TrimSpace(stringValue(message.Event["eventtype"]))
		if eventType == platform.EventDisconnected {
			out.CloseReason = "wecom server disconnected this connection because another connection was established"
			out.Terminal = true
			out.HealthUpdates = map[string]any{
				sdk.RuntimeHealthKeyStreamConnectionState: sdk.RuntimeHealthStateStopped,
				sdk.RuntimeHealthKeyStreamDisconnectedAt:  now,
			}
		}
		out.EventResult = &sdk.StreamEventResult{Type: "event", Ignored: true, Reason: firstString(eventType, "unsupported_event")}
		return out, nil
	default:
		out.EventResult = &sdk.StreamEventResult{Type: "frame", Ignored: true, Reason: "unsupported_command"}
		return out, nil
	}
}

func (c Connector) processMessageFrame(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, frame platform.Frame) (*EventResult, error) {
	if runtime.Gateway == nil {
		return nil, fmt.Errorf("wecom event handling requires sdk.Runtime.Gateway")
	}
	message, err := platform.DecodeMessage(frame.Body)
	if err != nil {
		return nil, err
	}
	botID := strings.TrimSpace(stringValue(account.Credential["bot_id"]))
	if botID != "" && strings.TrimSpace(message.BotID) != "" && botID != strings.TrimSpace(message.BotID) {
		return &EventResult{Type: "message", Ignored: true, Reason: "bot_id_mismatch"}, nil
	}
	accountUUID := accountKey(account)
	if accountUUID == "" {
		return nil, fmt.Errorf("wecom account_uuid or bot_id is required")
	}
	senderID := strings.TrimSpace(message.From.UserID)
	chatType, chatID := wecomChat(message)
	if chatType == "" || chatID == "" || senderID == "" || strings.TrimSpace(message.MessageID) == "" {
		return &EventResult{Type: "message", Ignored: true, Reason: "incomplete_message"}, nil
	}
	text := platform.MessageText(message)
	if text == "" && !supportedMessageType(message.MessageType) {
		return &EventResult{Type: "message", Ignored: true, Reason: "unsupported_message_type"}, nil
	}
	state, err := loadAccountState(ctx, runtime, account)
	if err != nil {
		return nil, err
	}
	dedupeKey := strings.Join([]string{Platform, accountUUID, strings.TrimSpace(message.MessageID)}, ":")
	inboundSeen := mapValue(state["inbound_seen"])
	peerSessions := mapValue(state["peer_sessions"])
	peerKey := sdk.ChatSourceID(Platform, accountUUID, chatType, chatID)
	if _, exists := inboundSeen[dedupeKey]; exists {
		return &EventResult{Type: "message", Ignored: true, Reason: "duplicate", SessionUUID: stringValue(peerSessions[peerKey])}, nil
	}

	inbound := buildInboundMessage(runtime, accountUUID, message, chatType, chatID, text, dedupeKey)
	sessionUUID, err := runtime.Gateway.EnsureChatSession(ctx, sdk.EnsureChatSessionRequest{
		WorkspaceUUID:       runtime.WorkspaceUUID,
		Platform:            Platform,
		AccountUUID:         accountUUID,
		ChatType:            chatType,
		ChatID:              chatID,
		ChatIdentity:        inbound.ChatIdentity,
		SenderID:            senderID,
		AgentParticipantID:  runtime.Gateway.AgentParticipantID(),
		BridgeParticipantID: runtime.Gateway.BridgeParticipantID(Platform),
		Metadata: map[string]any{
			"source":        Platform,
			"account_uuid":  accountUUID,
			"chat_identity": inbound.ChatIdentity,
		},
	})
	if err != nil {
		return nil, err
	}
	messageUUID, err := runtime.Gateway.CreateMessage(ctx, sdk.CreateMessageRequest{
		WorkspaceUUID: runtime.WorkspaceUUID,
		SessionUUID:   sessionUUID,
		SenderID:      sdk.IMPersonParticipantID(Platform, chatType, chatID, senderID),
		Content:       text,
		DedupeKey:     dedupeKey,
		Metadata: map[string]any{
			"source":             Platform,
			"platform":           Platform,
			"account_uuid":       accountUUID,
			"wecom_bot_id":       message.BotID,
			"wecom_message_id":   message.MessageID,
			"wecom_message_type": message.MessageType,
			"wecom_sender_id":    senderID,
			"wecom_chat_type":    message.ChatType,
			"wecom_chat_id":      chatID,
			"chat_identity":      inbound.ChatIdentity,
			"inbound_message":    inbound,
		},
	})
	if err != nil {
		return nil, err
	}

	peerSessions[peerKey] = sessionUUID
	inboundSeen[dedupeKey] = time.Now().UTC().Format(time.RFC3339Nano)
	pruneTrackedMap(inboundSeen, maxTrackedInbound)
	state["peer_sessions"] = peerSessions
	state["inbound_seen"] = inboundSeen
	if err := saveAccountState(ctx, runtime, accountUUID, state); err != nil {
		return nil, err
	}
	return &EventResult{Type: "message", SessionUUID: sessionUUID, MessageUUID: messageUUID, Inbound: &inbound}, nil
}

func buildInboundMessage(runtime sdk.Runtime, accountUUID string, message platform.Message, chatType, chatID, text, dedupeKey string) sdk.InboundMessage {
	botID := firstString(message.BotID, runtime.Account.Credential["bot_id"])
	mentionedMe := chatType == sdk.ChatTypeDirect || botID != ""
	mentions := []sdk.MentionIdentity(nil)
	if mentionedMe && botID != "" {
		mentions = []sdk.MentionIdentity{{ID: botID, IDType: "aibot_id"}}
	}
	return sdk.InboundMessage{
		WorkspaceUUID: runtime.WorkspaceUUID,
		Platform:      Platform,
		AccountUUID:   accountUUID,
		ChannelUUID:   runtime.Channel.UUID,
		ChatType:      chatType,
		ChatID:        chatID,
		ChatIdentity: sdk.ChatIdentity{
			ID:     chatID,
			IDType: "wecom_chat_id",
			Type:   chatType,
		},
		SenderID:          strings.TrimSpace(message.From.UserID),
		MessageID:         strings.TrimSpace(message.MessageID),
		Text:              text,
		ReferencedMessage: referencedMessage(message, chatType, chatID),
		DedupeKey:         dedupeKey,
		Mentions:          mentions,
		MentionedMe:       mentionedMe,
		Raw: map[string]any{
			"message_id":   message.MessageID,
			"message_type": message.MessageType,
			"bot_id":       message.BotID,
			"chat_type":    message.ChatType,
			"chat_id":      chatID,
			"sender_id":    message.From.UserID,
			"create_time":  message.CreateTime,
		},
	}
}

func referencedMessage(message platform.Message, chatType, chatID string) *sdk.ReferencedMessage {
	if message.Quote == nil {
		return nil
	}
	return &sdk.ReferencedMessage{
		Platform:    Platform,
		ChatType:    chatType,
		ChatID:      chatID,
		MessageType: strings.TrimSpace(message.Quote.MessageType),
		Text:        platform.QuoteText(message.Quote),
		Raw:         rawMap(message.Quote),
	}
}

func wecomChat(message platform.Message) (string, string) {
	switch strings.TrimSpace(message.ChatType) {
	case platform.ChatTypeGroup:
		return sdk.ChatTypeGroup, strings.TrimSpace(message.ChatID)
	case platform.ChatTypeSingle:
		return sdk.ChatTypeDirect, strings.TrimSpace(message.From.UserID)
	default:
		return "", ""
	}
}

func supportedMessageType(value string) bool {
	switch strings.TrimSpace(value) {
	case platform.MessageTypeText, platform.MessageTypeImage, platform.MessageTypeMixed, platform.MessageTypeVoice, platform.MessageTypeFile, platform.MessageTypeVideo:
		return true
	default:
		return false
	}
}

func streamEventResult(result *EventResult) *sdk.StreamEventResult {
	if result == nil {
		return nil
	}
	return &sdk.StreamEventResult{
		Type:        result.Type,
		Ignored:     result.Ignored,
		Reason:      result.Reason,
		SessionUUID: result.SessionUUID,
		MessageUUID: result.MessageUUID,
		Inbound:     result.Inbound,
	}
}

func decodeStreamState(value any) streamState {
	switch typed := value.(type) {
	case streamState:
		return typed
	case *streamState:
		if typed != nil {
			return *typed
		}
	case map[string]any:
		return streamState{AuthRequestID: stringValue(typed["auth_request_id"]), BotID: stringValue(typed["bot_id"])}
	}
	return streamState{}
}

func pruneTrackedMap(values map[string]any, limit int) {
	if limit <= 0 || len(values) <= limit {
		return
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return stringValue(values[keys[i]]) < stringValue(values[keys[j]]) })
	for len(values) > limit && len(keys) > 0 {
		delete(values, keys[0])
		keys = keys[1:]
	}
}
