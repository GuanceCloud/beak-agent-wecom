package beakwecom

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

const (
	wecomPingInterval        = 30 * time.Second
	wecomPongTimeout         = 10 * time.Second
	wecomMaxMediaBytes int64 = 32 << 20
)

func blockedMediaHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

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
	attachments, cleanupAttachments, attachmentErrors := downloadWeComAttachments(ctx, runtime.HTTPClient, message)
	if cleanupAttachments != nil {
		defer cleanupAttachments()
	}
	if strings.TrimSpace(text) == "" && (len(attachments) > 0 || len(attachmentErrors) > 0) {
		text = wecomAttachmentPlaceholder(attachments, attachmentErrors)
	}
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
	inbound.Attachments = attachments
	if len(attachmentErrors) > 0 {
		inbound.Raw["attachment_errors"] = attachmentErrors
	}
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
		Attachments:   inbound.Attachments,
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

func downloadWeComAttachments(ctx context.Context, client *http.Client, message platform.Message) ([]sdk.MediaAttachment, func(), []string) {
	type item struct {
		kind  string
		media *platform.MediaContent
	}
	items := make([]item, 0, 2)
	switch message.MessageType {
	case platform.MessageTypeImage:
		items = append(items, item{sdk.MediaKindImage, message.Image})
	case platform.MessageTypeFile:
		items = append(items, item{sdk.MediaKindFile, message.File})
	case platform.MessageTypeVoice:
		items = append(items, item{sdk.MediaKindAudio, mediaContentFromText(message.Voice)})
	case platform.MessageTypeVideo:
		items = append(items, item{sdk.MediaKindVideo, message.Video})
	case platform.MessageTypeMixed:
		for _, mixed := range message.Mixed.Items {
			if mixed.Image != nil {
				items = append(items, item{sdk.MediaKindImage, mixed.Image})
			}
		}
	}
	attachments := make([]sdk.MediaAttachment, 0, len(items))
	cleanups := make([]func(), 0, len(items))
	var errs []string
	cleanup := func() {
		for index := len(cleanups) - 1; index >= 0; index-- {
			if cleanups[index] != nil {
				cleanups[index]()
			}
		}
	}
	for _, current := range items {
		if current.media == nil || strings.TrimSpace(current.media.URL) == "" {
			errs = append(errs, "wecom media URL is missing")
			continue
		}
		path, remove, err := downloadWeComURL(ctx, client, current.media.URL, current.media.AESKey)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		cleanups = append(cleanups, remove)
		attachments = append(attachments, sdk.MediaAttachment{ID: current.media.URL, Kind: current.kind, Source: sdk.MediaSourcePath, Path: path, PlatformResourceID: current.media.URL})
	}
	return attachments, cleanup, errs
}

func mediaContentFromText(value *platform.TextContent) *platform.MediaContent {
	if value == nil || strings.TrimSpace(value.Content) == "" {
		return nil
	}
	return &platform.MediaContent{URL: value.Content}
}

func downloadWeComURL(ctx context.Context, client *http.Client, rawURL, aesKey string) (string, func(), error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" || parsed.Scheme != "https" {
		return "", nil, fmt.Errorf("wecom media URL must be HTTPS")
	}
	if blockedMediaHost(parsed.Hostname()) {
		return "", nil, fmt.Errorf("wecom media URL host is not allowed")
	}
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil, fmt.Errorf("wecom media download failed: status=%d", resp.StatusCode)
	}
	tmp, err := os.CreateTemp("", "beak-wecom-media-*")
	if err != nil {
		return "", nil, err
	}
	path := tmp.Name()
	cleanup := func() { _ = os.Remove(path) }
	data, err := io.ReadAll(io.LimitReader(resp.Body, wecomMaxMediaBytes+1))
	if err != nil {
		_ = tmp.Close()
		cleanup()
		return "", nil, err
	}
	if strings.TrimSpace(aesKey) != "" {
		data, err = decryptWeComMedia(data, aesKey)
		if err != nil {
			_ = tmp.Close()
			cleanup()
			return "", nil, err
		}
	}
	if int64(len(data)) > wecomMaxMediaBytes {
		_ = tmp.Close()
		cleanup()
		return "", nil, fmt.Errorf("wecom media exceeds %d bytes", wecomMaxMediaBytes)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", nil, err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

func decryptWeComMedia(ciphertext []byte, rawKey string) ([]byte, error) {
	keyText := strings.TrimSpace(rawKey)
	key, err := base64.StdEncoding.DecodeString(keyText)
	if err != nil {
		key, err = base64.RawStdEncoding.DecodeString(strings.TrimRight(keyText, "="))
	}
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("wecom media AES key must decode to 32 bytes")
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("wecom media ciphertext has invalid size")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	plaintext := make([]byte, len(ciphertext))
	iv := key[:aes.BlockSize]
	decrypter := cipher.NewCBCDecrypter(block, iv)
	decrypter.CryptBlocks(plaintext, ciphertext)
	padding := int(plaintext[len(plaintext)-1])
	if padding == 0 || padding > len(plaintext) || padding > 32 {
		return nil, fmt.Errorf("wecom media has invalid PKCS padding")
	}
	for _, value := range plaintext[len(plaintext)-padding:] {
		if int(value) != padding {
			return nil, fmt.Errorf("wecom media has invalid PKCS padding")
		}
	}
	return plaintext[:len(plaintext)-padding], nil
}

func wecomAttachmentPlaceholder(attachments []sdk.MediaAttachment, errors []string) string {
	labels := make([]string, 0, len(attachments)+len(errors))
	for _, attachment := range attachments {
		label := "附件"
		switch attachment.Kind {
		case sdk.MediaKindImage:
			label = "图片"
		case sdk.MediaKindAudio:
			label = "音频"
		case sdk.MediaKindVideo:
			label = "视频"
		case sdk.MediaKindFile:
			label = "文件"
		}
		labels = append(labels, "["+label+"]")
	}
	for range errors {
		labels = append(labels, "[附件下载失败]")
	}
	return strings.Join(labels, " ")
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
