package beakwecom

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

func TestMetadataAndCredentialSchema(t *testing.T) {
	connector := Connector{}
	metadata := connector.Metadata()
	if metadata.Platform != Platform || metadata.ID != ID {
		t.Fatalf("metadata=%+v", metadata)
	}
	if metadata.Capabilities.RuntimeOwnership != sdk.RuntimeOwnershipHostStream || !metadata.Capabilities.Stream || metadata.Capabilities.Webhook {
		t.Fatalf("capabilities=%+v", metadata.Capabilities)
	}
	schema := connector.CredentialSchema(context.Background())
	if len(schema.Required) != 2 || schema.Properties["bot_id"].Secret || !schema.Properties["secret"].Secret {
		t.Fatalf("schema=%+v", schema)
	}
}

func TestValidateCredentialUsesRealWebSocketAuthentication(t *testing.T) {
	endpoint, closeServer := newCredentialServer(t)
	defer closeServer()

	connector := Connector{}
	valid, err := connector.ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{"bot_id": "bot-1", "secret": "good"},
		Runtime:    sdk.Runtime{Native: NativeRuntime{WebSocketURL: endpoint}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !valid.Valid || valid.AccountKey != "bot-1" || valid.State["bot_id"] != "bot-1" {
		t.Fatalf("valid result=%+v", valid)
	}

	invalid, err := connector.ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{"bot_id": "bot-1", "secret": "bad"},
		Runtime:    sdk.Runtime{Native: NativeRuntime{WebSocketURL: endpoint}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if invalid.Valid || !strings.Contains(invalid.Error, "authentication failed") {
		t.Fatalf("invalid result=%+v", invalid)
	}
}

func TestHostStreamAuthenticationHeartbeatAndInbound(t *testing.T) {
	connector := Connector{}
	account := testAccount()
	gateway := &testGateway{}
	store := &testAccountStore{}
	runtime := testRuntime(account, gateway, store)
	connected, err := connector.ConnectStream(context.Background(), runtime, account)
	if err != nil {
		t.Fatal(err)
	}
	if !connected.WaitForReady || len(connected.InitialFrames) != 1 || connected.ReadMessageType != sdk.StreamMessageTypeText {
		t.Fatalf("connect result=%+v", connected)
	}
	auth := mustParseFrame(t, connected.InitialFrames[0].Data)
	if auth.Command != platform.CommandSubscribe || auth.Body["bot_id"] != "bot-1" || auth.Body["secret"] != "secret-1" {
		t.Fatalf("auth frame=%+v", auth)
	}
	authFailure, err := connector.HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{
		MessageType: sdk.StreamMessageTypeText,
		Data:        mustFrameData(t, platform.Frame{Headers: platform.FrameHeaders{RequestID: auth.Headers.RequestID}, ErrCode: 40001, ErrMsg: "invalid secret"}),
		State:       connected.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !authFailure.Terminal || authFailure.CloseReason == "" || authFailure.HealthUpdates[sdk.RuntimeHealthKeyStreamConnectionState] != sdk.RuntimeHealthStateReconnectFailed {
		t.Fatalf("auth failure result=%+v", authFailure)
	}

	authAck := mustFrameData(t, platform.Frame{Headers: platform.FrameHeaders{RequestID: auth.Headers.RequestID}, ErrCode: 0, ErrMsg: "ok"})
	authResult, err := connector.HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{
		MessageType: sdk.StreamMessageTypeText,
		Data:        authAck,
		State:       connected.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !authResult.Ready || authResult.HealthUpdates[sdk.RuntimeHealthKeyStreamConnectionState] != sdk.RuntimeHealthStateConnected {
		t.Fatalf("auth result=%+v", authResult)
	}

	ping, err := connector.BuildStreamPing(context.Background(), sdk.StreamPingRequest{State: connected.State})
	if err != nil {
		t.Fatal(err)
	}
	pingFrame := mustParseFrame(t, ping.Data)
	pongResult, err := connector.HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{
		MessageType: sdk.StreamMessageTypeText,
		Data:        mustFrameData(t, platform.Frame{Headers: platform.FrameHeaders{RequestID: pingFrame.Headers.RequestID}, ErrCode: 0}),
		State:       connected.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pongResult.HealthUpdates[sdk.RuntimeHealthKeyStreamLastPongAt] == nil {
		t.Fatalf("pong result=%+v", pongResult)
	}

	inboundFrame := platform.Frame{
		Command: platform.CommandMessage,
		Headers: platform.FrameHeaders{RequestID: "callback-1"},
		Body: map[string]any{
			"msgid":        "msg-1",
			"aibotid":      "bot-1",
			"chatid":       "group-1",
			"chattype":     "group",
			"from":         map[string]any{"userid": "user-1"},
			"msgtype":      "mixed",
			"response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response",
			"mixed": map[string]any{"msg_item": []any{
				map[string]any{"msgtype": "text", "text": map[string]any{"content": "hello"}},
				map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://example.test/image"}},
			}},
			"quote": map[string]any{"msgtype": "text", "text": map[string]any{"content": "quoted"}},
		},
	}
	result, err := connector.HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{
		MessageType: sdk.StreamMessageTypeText,
		Data:        mustFrameData(t, inboundFrame),
		State:       connected.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.EventResult == nil || result.EventResult.Inbound == nil {
		t.Fatalf("event result=%+v", result)
	}
	inbound := result.EventResult.Inbound
	if inbound.ChatType != sdk.ChatTypeGroup || inbound.ChatID != "group-1" || inbound.SenderID != "user-1" {
		t.Fatalf("inbound identity=%+v", inbound)
	}
	if inbound.Text != "hello\n[图片]" || inbound.ReferencedMessage == nil || inbound.ReferencedMessage.Text != "quoted" {
		t.Fatalf("inbound content=%+v", inbound)
	}
	if !inbound.MentionedMe || result.HealthUpdates[sdk.RuntimeHealthKeyStreamLastEventAt] == nil {
		t.Fatalf("inbound flags/result=%+v", result)
	}
	if len(gateway.messages) != 1 {
		t.Fatalf("messages=%+v", gateway.messages)
	}
	if _, ok := gateway.messages[0].Metadata["wecom_response_url"]; ok {
		t.Fatalf("temporary response_url must not be persisted: metadata=%+v", gateway.messages[0].Metadata)
	}
	if raw, _ := gateway.messages[0].Metadata["inbound_message"].(sdk.InboundMessage); raw.Raw["response_url"] != nil {
		t.Fatalf("temporary response_url must not be exposed in normalized raw metadata: raw=%+v", raw.Raw)
	}

	duplicate, err := connector.HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{
		MessageType: sdk.StreamMessageTypeText,
		Data:        mustFrameData(t, inboundFrame),
		State:       connected.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.EventResult == nil || !duplicate.EventResult.Ignored || duplicate.EventResult.Reason != "duplicate" || len(gateway.messages) != 1 {
		t.Fatalf("duplicate result=%+v messages=%d", duplicate, len(gateway.messages))
	}

	disconnected, err := connector.HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{
		MessageType: sdk.StreamMessageTypeText,
		Data: mustFrameData(t, platform.Frame{
			Command: platform.CommandEvent,
			Headers: platform.FrameHeaders{RequestID: "event-1"},
			Body: map[string]any{
				"msgid":    "event-1",
				"aibotid":  "bot-1",
				"chattype": "single",
				"from":     map[string]any{"userid": "system"},
				"msgtype":  "event",
				"event":    map[string]any{"eventtype": platform.EventDisconnected},
			},
		}),
		State: connected.State,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !disconnected.Terminal || disconnected.CloseReason == "" || disconnected.HealthUpdates[sdk.RuntimeHealthKeyStreamConnectionState] != sdk.RuntimeHealthStateStopped {
		t.Fatalf("disconnected result=%+v", disconnected)
	}
}

func TestSendUsesHostStreamAndSplitsUTF8Safely(t *testing.T) {
	account := testAccount()
	transport := &testStreamTransport{}
	runtime := testRuntime(account, &testGateway{}, &testAccountStore{})
	runtime.Stream = transport
	content := strings.Repeat("你", platform.MaxMarkdownBytes/3+10)
	result, err := (Connector{}).Send(context.Background(), runtime, sdk.OutboundMessage{
		AccountUUID: account.UUID,
		ChatType:    sdk.ChatTypeDirect,
		ChatID:      "user-1",
		Text:        content,
		Format:      "text",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Platform != Platform || len(transport.requests) != 2 {
		t.Fatalf("result=%+v requests=%d", result, len(transport.requests))
	}
	var rebuilt strings.Builder
	for _, request := range transport.requests {
		frame := mustParseFrame(t, request.Frame.Data)
		if frame.Command != platform.CommandSendMessage || frame.Body["chatid"] != "user-1" {
			t.Fatalf("send frame=%+v", frame)
		}
		markdown, _ := frame.Body["markdown"].(map[string]any)
		chunk := stringValue(markdown["content"])
		if len([]byte(chunk)) > platform.MaxMarkdownBytes || !json.Valid(request.Frame.Data) {
			t.Fatalf("invalid chunk bytes=%d", len([]byte(chunk)))
		}
		rebuilt.WriteString(chunk)
	}
	if rebuilt.String() != content {
		t.Fatal("split content was not reconstructed exactly")
	}
}

func TestStartWiresChannelAndAccountSession(t *testing.T) {
	account := testAccount()
	gateway := &testGateway{}
	store := &testAccountStore{}
	if err := (Connector{}).Start(context.Background(), testRuntime(account, gateway, store)); err != nil {
		t.Fatal(err)
	}
	if gateway.ensureChannelCalls != 1 || gateway.ensureLinkCalls != 1 {
		t.Fatalf("gateway calls channel=%d link=%d", gateway.ensureChannelCalls, gateway.ensureLinkCalls)
	}
	state, _ := store.LoadChannelAccountState(context.Background(), account.UUID)
	if state["channel_link_session"] != "link-session" || state["bot_id"] != "bot-1" {
		t.Fatalf("state=%+v", state)
	}
}

func newCredentialServer(t *testing.T) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		frame, err := platform.ParseFrame(data)
		if err != nil {
			return
		}
		code := 0
		message := "ok"
		if frame.Body["secret"] == "bad" {
			code = 40001
			message = "invalid secret"
		}
		_ = conn.WriteMessage(websocket.TextMessage, mustFrameData(t, platform.Frame{
			Headers: platform.FrameHeaders{RequestID: frame.Headers.RequestID},
			ErrCode: code,
			ErrMsg:  message,
		}))
	}))
	return "ws" + strings.TrimPrefix(server.URL, "http"), server.Close
}

func testAccount() sdk.ChannelAccount {
	return sdk.ChannelAccount{
		UUID:          "account-1",
		WorkspaceUUID: "workspace-1",
		ChannelUUID:   "channel-1",
		Platform:      Platform,
		Credential:    map[string]any{"bot_id": "bot-1", "secret": "secret-1", "account_id": "bot-1"},
		State:         map[string]any{},
	}
}

func testRuntime(account sdk.ChannelAccount, gateway sdk.Gateway, store sdk.AccountStore) sdk.Runtime {
	return sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Accounts:      []sdk.ChannelAccount{account},
		Gateway:       gateway,
		AccountStore:  store,
	}
}

func mustFrameData(t *testing.T, frame platform.Frame) []byte {
	t.Helper()
	data, err := platform.MarshalFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustParseFrame(t *testing.T, data []byte) platform.Frame {
	t.Helper()
	frame, err := platform.ParseFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

type testGateway struct {
	mu                 sync.Mutex
	ensureChannelCalls int
	ensureLinkCalls    int
	messages           []sdk.CreateMessageRequest
}

func (g *testGateway) EnsureChannel(context.Context, sdk.EnsureChannelRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ensureChannelCalls++
	return "channel-1", nil
}

func (g *testGateway) EnsureChannelLinkSession(context.Context, sdk.EnsureChannelLinkSessionRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ensureLinkCalls++
	return "link-session", nil
}

func (*testGateway) EnsureChatSession(context.Context, sdk.EnsureChatSessionRequest) (string, error) {
	return "chat-session", nil
}

func (g *testGateway) CreateMessage(_ context.Context, req sdk.CreateMessageRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.messages = append(g.messages, req)
	return "message-uuid", nil
}

func (*testGateway) StreamSession(context.Context, sdk.StreamSessionRequest, func(sdk.StreamEvent) error) error {
	return nil
}

func (*testGateway) AgentParticipantID() string { return "agent-1" }

func (*testGateway) BridgeParticipantID(platform string) string {
	return sdk.BridgeParticipantID(platform)
}

type testAccountStore struct {
	mu    sync.Mutex
	state map[string]map[string]any
}

func (s *testAccountStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMap(s.ensure(accountUUID)), nil
}

func (s *testAccountStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.ensure(accountUUID)
	for key, value := range state {
		current[key] = value
	}
	return nil
}

func (s *testAccountStore) ensure(accountUUID string) map[string]any {
	if s.state == nil {
		s.state = map[string]map[string]any{}
	}
	if s.state[accountUUID] == nil {
		s.state[accountUUID] = map[string]any{}
	}
	return s.state[accountUUID]
}

type testStreamTransport struct {
	mu       sync.Mutex
	requests []sdk.StreamRequest
}

func (s *testStreamTransport) Request(_ context.Context, req sdk.StreamRequest) (*sdk.StreamResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()
	return &sdk.StreamResponse{
		CorrelationID: req.CorrelationID,
		Frame: sdk.StreamFrame{
			MessageType: sdk.StreamMessageTypeText,
			Data: mustProtocolFrame(platform.Frame{
				Headers: platform.FrameHeaders{RequestID: req.CorrelationID},
				ErrCode: 0,
				Body:    map[string]any{"msgid": "sent-" + req.CorrelationID},
			}),
		},
	}, nil
}

func mustProtocolFrame(frame platform.Frame) []byte {
	data, err := platform.MarshalFrame(frame)
	if err != nil {
		panic(err)
	}
	return data
}

func TestValidationHonorsContextDeadline(t *testing.T) {
	endpoint, closeServer := newCredentialServer(t)
	defer closeServer()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := (Connector{}).ValidateCredential(ctx, sdk.CredentialValidationRequest{
		Credential: map[string]any{"bot_id": "bot-1", "secret": "good"},
		Runtime:    sdk.Runtime{Native: NativeRuntime{WebSocketURL: endpoint}},
	})
	if err != nil || result == nil || !result.Valid {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestValidationCancellationClosesPendingWebSocketRead(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		close(connected)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (Connector{}).ValidateCredential(ctx, sdk.CredentialValidationRequest{
			Credential: map[string]any{"bot_id": "bot-1", "secret": "good"},
			Runtime: sdk.Runtime{Native: NativeRuntime{
				WebSocketURL: "ws" + strings.TrimPrefix(server.URL, "http"),
			}},
		})
		done <- err
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("credential validation did not send the authentication frame")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("credential validation did not stop after context cancellation")
	}
}
