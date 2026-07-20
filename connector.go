package beakwecom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

const (
	credentialValidationTimeout = 10 * time.Second
	streamRequestTimeout        = 10 * time.Second
	maxTrackedInbound           = 4096
)

var ErrCredentialLogin = errors.New("wecom connector uses credential login; create channel account from CredentialSchema")

// NativeRuntime is an optional test/deployment override. Production callers
// normally leave it empty and use the official WeCom WebSocket endpoint.
type NativeRuntime struct {
	WebSocketURL string
	Dialer       *websocket.Dialer
}

type Connector struct{ channel Channel }

type EventResult struct {
	Type        string              `json:"type"`
	Ignored     bool                `json:"ignored,omitempty"`
	Reason      string              `json:"reason,omitempty"`
	SessionUUID string              `json:"session_uuid,omitempty"`
	MessageUUID string              `json:"message_uuid,omitempty"`
	Inbound     *sdk.InboundMessage `json:"inbound,omitempty"`
}

func NewConnector() sdk.Connector { return Connector{channel: Channel{}} }

var _ sdk.Connector = Connector{}
var _ sdk.HostStreamConnector = Connector{}

func (c Connector) Metadata() sdk.ConnectorMetadata {
	meta := c.channel.Metadata()
	caps := c.channel.Capabilities()
	return sdk.ConnectorMetadata{
		ID:          meta.ID,
		Platform:    meta.Platform,
		Label:       meta.Label,
		Description: meta.Description,
		Capabilities: sdk.Capabilities{
			LoginModes:       []string{sdk.LoginModeCredential},
			Text:             caps.Text,
			Media:            caps.Media,
			GroupChat:        caps.GroupChat,
			DirectChat:       caps.DirectChat,
			Stream:           true,
			Webhook:          false,
			BlockStreaming:   caps.BlockStreaming,
			AckModes:         nil,
			RuntimeOwnership: sdk.RuntimeOwnershipHostStream,
		},
	}
}

func (c Connector) CredentialSchema(context.Context) sdk.CredentialSchema {
	schema := c.channel.SettingsSchema()
	properties := make(map[string]sdk.CredentialField, len(schema.Properties))
	for key, raw := range schema.Properties {
		item, _ := raw.(map[string]any)
		properties[key] = sdk.CredentialField{
			Type:        stringValue(item["type"]),
			Title:       stringValue(item["title"]),
			Description: stringValue(item["description"]),
			Secret:      boolValue(item["secret"]),
		}
	}
	return sdk.CredentialSchema{
		Type:                 schema.Type,
		LoginModes:           []string{sdk.LoginModeCredential},
		Properties:           properties,
		Required:             append([]string(nil), schema.Required...),
		AdditionalProperties: false,
	}
}

func (Connector) ValidateCredential(ctx context.Context, req sdk.CredentialValidationRequest) (*sdk.CredentialValidationResult, error) {
	credential := cloneMap(req.Credential)
	state := cloneMap(req.State)
	botID := strings.TrimSpace(stringValue(credential["bot_id"]))
	secret := strings.TrimSpace(stringValue(credential["secret"]))
	if botID == "" || secret == "" {
		return credentialValidationFailure(credential, state, "bot_id and secret are required"), nil
	}
	credential["bot_id"] = botID
	credential["secret"] = secret

	native := nativeRuntime(req.Runtime.Native)
	endpoint := strings.TrimSpace(native.WebSocketURL)
	if endpoint == "" {
		endpoint = platform.DefaultWebSocketURL
	}
	result, err := validateBotCredential(ctx, native.Dialer, endpoint, botID, secret)
	if err != nil {
		return nil, fmt.Errorf("wecom credential validation failed: %w", err)
	}
	if result.ErrCode != 0 {
		return credentialValidationFailure(credential, state, fmt.Sprintf("authentication failed: code=%d message=%s", result.ErrCode, strings.TrimSpace(result.ErrMsg))), nil
	}

	credential["account_id"] = botID
	state["account_id"] = botID
	state["bot_id"] = botID
	identities := []map[string]any{{"id": botID, "id_type": "aibot_id"}}
	state["bot_identity"] = identities[0]
	state["bot_identities"] = identities
	return &sdk.CredentialValidationResult{
		Valid:       true,
		AccountKey:  botID,
		DisplayName: firstString(credential["display_name"], botID),
		Credential:  credential,
		State:       state,
		Metadata: map[string]any{
			"platform":         Platform,
			"bot_id":           botID,
			"validation_scope": "websocket_auth",
		},
	}, nil
}

func validateBotCredential(ctx context.Context, dialer *websocket.Dialer, endpoint, botID, secret string) (platform.Frame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, credentialValidationTimeout)
		defer cancel()
	}
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, response, err := dialer.DialContext(ctx, endpoint, http.Header{})
	if response != nil && response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil {
		return platform.Frame{}, err
	}
	defer func() { _ = conn.Close() }()
	stopCloseOnCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCloseOnCancel()

	requestID, err := platform.NewRequestID(platform.CommandSubscribe)
	if err != nil {
		return platform.Frame{}, err
	}
	payload, err := platform.MarshalFrame(platform.AuthFrame(requestID, botID, secret))
	if err != nil {
		return platform.Frame{}, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		_ = conn.SetReadDeadline(deadline)
	}
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return platform.Frame{}, ctxErr
		}
		return platform.Frame{}, err
	}
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return platform.Frame{}, ctxErr
			}
			return platform.Frame{}, err
		}
		if messageType != websocket.TextMessage {
			continue
		}
		frame, err := platform.ParseFrame(data)
		if err != nil {
			return platform.Frame{}, err
		}
		if frame.Headers.RequestID == requestID {
			return frame, nil
		}
	}
}

func credentialValidationFailure(credential, state map[string]any, message string) *sdk.CredentialValidationResult {
	return &sdk.CredentialValidationResult{
		Valid:       false,
		AccountKey:  firstString(credential["account_id"], credential["bot_id"]),
		DisplayName: firstString(credential["display_name"], credential["bot_id"]),
		Credential:  credential,
		State:       state,
		Metadata:    map[string]any{"platform": Platform},
		Error:       strings.TrimSpace(message),
	}
}

func (Connector) StartLogin(context.Context, sdk.LoginStartRequest) (*sdk.LoginChallenge, error) {
	return nil, ErrCredentialLogin
}

func (Connector) PollLogin(context.Context, sdk.LoginPollRequest) (*sdk.LoginStatus, error) {
	return nil, ErrCredentialLogin
}

func (Connector) Start(ctx context.Context, runtime sdk.Runtime) error {
	if runtime.Gateway == nil {
		return fmt.Errorf("wecom connector requires sdk.Runtime.Gateway")
	}
	if _, err := runtime.Gateway.EnsureChannel(ctx, sdk.EnsureChannelRequest{
		WorkspaceUUID: runtime.WorkspaceUUID,
		Platform:      Platform,
		Name:          "企业微信",
		Config:        map[string]any{"bridge": ID},
	}); err != nil {
		return err
	}
	for _, account := range runtimeAccountCandidates(runtime) {
		accountUUID := accountKey(account)
		if accountUUID == "" {
			return fmt.Errorf("wecom account_uuid or bot_id is required")
		}
		sessionUUID, err := runtime.Gateway.EnsureChannelLinkSession(ctx, sdk.EnsureChannelLinkSessionRequest{
			WorkspaceUUID:       runtime.WorkspaceUUID,
			Platform:            Platform,
			AccountUUID:         accountUUID,
			AgentParticipantID:  runtime.Gateway.AgentParticipantID(),
			BridgeParticipantID: runtime.Gateway.BridgeParticipantID(Platform),
		})
		if err != nil {
			return err
		}
		state, err := loadAccountState(ctx, runtime, account)
		if err != nil {
			return err
		}
		botID := strings.TrimSpace(stringValue(account.Credential["bot_id"]))
		state["account_id"] = firstString(state["account_id"], botID)
		state["bot_id"] = botID
		state["channel_link_session"] = sessionUUID
		if botID != "" {
			identity := map[string]any{"id": botID, "id_type": "aibot_id"}
			state["bot_identity"] = identity
			state["bot_identities"] = []map[string]any{identity}
		}
		if err := saveAccountState(ctx, runtime, accountUUID, state); err != nil {
			return err
		}
	}
	return nil
}

func (Connector) Send(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	accountUUID := accountKey(account)
	if strings.TrimSpace(req.ChatID) == "" {
		return nil, fmt.Errorf("wecom outbound chat_id is required")
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("wecom outbound text is required")
	}
	if runtime.Stream == nil {
		return nil, fmt.Errorf("wecom host stream transport is unavailable")
	}

	chunks := splitUTF8(req.Text, platform.MaxMarkdownBytes)
	messageIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		requestID, err := platform.NewRequestID(platform.CommandSendMessage)
		if err != nil {
			return nil, err
		}
		payload, err := platform.MarshalFrame(platform.SendMarkdownFrame(requestID, req.ChatID, chunk))
		if err != nil {
			return nil, err
		}
		response, err := runtime.Stream.Request(ctx, sdk.StreamRequest{
			Frame:         sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload},
			CorrelationID: requestID,
			Timeout:       streamRequestTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("wecom send message: %w", err)
		}
		if response == nil {
			return nil, fmt.Errorf("wecom send message returned empty response")
		}
		ack, err := platform.ParseFrame(response.Frame.Data)
		if err != nil {
			return nil, err
		}
		if ack.Headers.RequestID != requestID {
			return nil, fmt.Errorf("wecom send response req_id mismatch: got %q want %q", ack.Headers.RequestID, requestID)
		}
		if ack.ErrCode != 0 {
			return nil, fmt.Errorf("wecom send message failed: code=%d message=%s", ack.ErrCode, strings.TrimSpace(ack.ErrMsg))
		}
		messageIDs = append(messageIDs, firstString(ack.Body["msgid"], requestID))
	}
	return &sdk.SendResult{
		Platform:    Platform,
		AccountUUID: accountUUID,
		MessageID:   messageIDs[len(messageIDs)-1],
		Raw: map[string]any{
			"message_ids":     messageIDs,
			"delivery_method": "host_stream",
			"msg_type":        "markdown",
			"chunk_count":     len(chunks),
		},
	}, nil
}

func (Connector) Acknowledge(_ context.Context, _ sdk.Runtime, req sdk.OutboundAck) (*sdk.AckResult, error) {
	return &sdk.AckResult{
		Platform:    Platform,
		AccountUUID: strings.TrimSpace(req.AccountUUID),
		Mode:        strings.TrimSpace(req.Mode),
		Status:      "unsupported",
		Raw:         map[string]any{"reason": "unsupported_ack_mode"},
	}, nil
}

func (Connector) Stop(context.Context, sdk.ChannelAccount) error { return nil }

func splitUTF8(value string, maxBytes int) []string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return []string{value}
	}
	chunks := make([]string, 0, len(value)/maxBytes+1)
	start := 0
	for start < len(value) {
		end := start + maxBytes
		if end >= len(value) {
			chunks = append(chunks, value[start:])
			break
		}
		for end > start && !utf8.RuneStart(value[end]) {
			end--
		}
		if end == start {
			_, size := utf8.DecodeRuneInString(value[start:])
			end = start + size
		}
		chunks = append(chunks, value[start:end])
		start = end
	}
	return chunks
}

func nativeRuntime(value any) NativeRuntime {
	switch typed := value.(type) {
	case NativeRuntime:
		return typed
	case *NativeRuntime:
		if typed != nil {
			return *typed
		}
	}
	return NativeRuntime{}
}

func runtimeAccountCandidates(runtime sdk.Runtime) []sdk.ChannelAccount {
	values := make([]sdk.ChannelAccount, 0, len(runtime.Accounts)+1)
	seen := map[string]struct{}{}
	appendAccount := func(account sdk.ChannelAccount) {
		key := accountKey(account)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		values = append(values, account)
	}
	appendAccount(runtime.Account)
	for _, account := range runtime.Accounts {
		appendAccount(account)
	}
	return values
}

func selectRuntimeAccount(runtime sdk.Runtime, requested string) (sdk.ChannelAccount, error) {
	requested = strings.TrimSpace(requested)
	for _, account := range runtimeAccountCandidates(runtime) {
		if requested == "" || requested == accountKey(account) || requested == strings.TrimSpace(account.UUID) {
			return account, nil
		}
	}
	return sdk.ChannelAccount{}, fmt.Errorf("wecom account %q is not available", requested)
}

func accountKey(account sdk.ChannelAccount) string {
	return firstString(account.UUID, account.Credential["account_id"], account.Credential["bot_id"], account.State["account_id"], account.State["bot_id"])
}

func loadAccountState(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount) (map[string]any, error) {
	state := cloneMap(account.State)
	if runtime.AccountStore == nil || strings.TrimSpace(accountKey(account)) == "" {
		return state, nil
	}
	stored, err := runtime.AccountStore.LoadChannelAccountState(ctx, accountKey(account))
	if err != nil {
		return nil, err
	}
	for key, value := range stored {
		state[key] = value
	}
	return state, nil
}

func saveAccountState(ctx context.Context, runtime sdk.Runtime, accountUUID string, state map[string]any) error {
	if runtime.AccountStore == nil {
		return nil
	}
	state["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	return runtime.AccountStore.SaveChannelAccountState(ctx, accountUUID, state)
}

func cloneMap(values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func mapValue(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	default:
		return map[string]any{}
	}
}

func rawMap(value any) map[string]any {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func firstString(values ...any) string {
	for _, value := range values {
		if text := strings.TrimSpace(stringValue(value)); text != "" {
			return text
		}
	}
	return ""
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

func boolValue(value any) bool {
	valueBool, _ := value.(bool)
	return valueBool
}
