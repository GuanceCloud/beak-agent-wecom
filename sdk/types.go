package sdk

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

const (
	ChatTypeGroup  = "group"
	ChatTypeDirect = "direct"

	LoginModeQRCode     = "qr_code"
	LoginModeCredential = "credential"

	RuntimeOwnershipHostStream = "host_stream"
	RuntimeOwnershipSDKOwned   = "sdk_owned"

	WebhookRegistrationManual    = "manual"
	WebhookRegistrationAutomatic = "automatic"

	StreamMessageTypeText   = 1
	StreamMessageTypeBinary = 2
	StreamMessageTypePing   = 9
)

type Connector interface {
	Metadata() ConnectorMetadata
	CredentialSchema(ctx context.Context) CredentialSchema
	ValidateCredential(ctx context.Context, req CredentialValidationRequest) (*CredentialValidationResult, error)
	StartLogin(ctx context.Context, req LoginStartRequest) (*LoginChallenge, error)
	PollLogin(ctx context.Context, req LoginPollRequest) (*LoginStatus, error)
	Start(ctx context.Context, runtime Runtime) error
	Send(ctx context.Context, runtime Runtime, req OutboundMessage) (*SendResult, error)
	Acknowledge(ctx context.Context, runtime Runtime, req OutboundAck) (*AckResult, error)
	Stop(ctx context.Context, account ChannelAccount) error
}

type WebhookResponse struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
}

// WebhookError carries structured HTTP semantics across the SDK/host boundary.
// Hosts should inspect HTTPStatusCode and ErrorCode instead of matching Error()
// text, which remains display-only.
type WebhookError struct {
	StatusCode int
	Code       string
	Err        error
}

func (e *WebhookError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return http.StatusText(e.StatusCode)
}

func (e *WebhookError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *WebhookError) HTTPStatusCode() int {
	if e == nil {
		return 0
	}
	return e.StatusCode
}

func (e *WebhookError) ErrorCode() string {
	if e == nil {
		return ""
	}
	return e.Code
}

// HostStreamConnector is implemented by SDKs whose platform stream connection is
// owned by the Beak host while platform endpoint and frame semantics stay inside
// the SDK. Implementations must not start their own long-running reconnect loop.
type HostStreamConnector interface {
	ConnectStream(ctx context.Context, runtime Runtime, account ChannelAccount) (*StreamConnectResult, error)
	BuildStreamPing(ctx context.Context, req StreamPingRequest) (*StreamFrame, error)
	HandleStreamFrame(ctx context.Context, runtime Runtime, account ChannelAccount, req StreamFrameRequest) (*StreamFrameResult, error)
}

type StreamConnectResult struct {
	URL             string
	Headers         map[string]string
	ServiceID       string
	ReadMessageType int
	InitialFrames   []StreamFrame
	WaitForReady    bool
	PingInterval    time.Duration
	PongTimeout     time.Duration
	State           any
	HealthUpdates   map[string]any
}

type StreamPingRequest struct {
	ServiceID string
	State     any
}

type StreamFrameRequest struct {
	MessageType int
	Data        []byte
	ServiceID   string
	State       any
}

type StreamFrame struct {
	MessageType int
	Data        []byte
}

type StreamFrameResult struct {
	ResponseFrames []StreamFrame
	ResponseTo     string
	Ready          bool
	Terminal       bool
	HealthUpdates  map[string]any
	EventResult    *StreamEventResult
	CloseReason    string
	PingInterval   time.Duration
	PongTimeout    time.Duration
	State          any
}

type StreamEventResult struct {
	Type        string          `json:"type"`
	Ignored     bool            `json:"ignored,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	SessionUUID string          `json:"session_uuid,omitempty"`
	MessageUUID string          `json:"message_uuid,omitempty"`
	Inbound     *InboundMessage `json:"inbound,omitempty"`
}

type ConnectorMetadata struct {
	ID           string       `json:"id"`
	Platform     string       `json:"platform"`
	Label        string       `json:"label"`
	Description  string       `json:"description,omitempty"`
	Capabilities Capabilities `json:"capabilities"`
}

type Capabilities struct {
	LoginModes          []string `json:"login_modes"`
	Text                bool     `json:"text"`
	Media               bool     `json:"media"`
	GroupChat           bool     `json:"group_chat"`
	DirectChat          bool     `json:"direct_chat"`
	Stream              bool     `json:"stream"`
	Webhook             bool     `json:"webhook"`
	WebhookRegistration string   `json:"webhook_registration,omitempty"`
	BlockStreaming      bool     `json:"block_streaming"`
	AckModes            []string `json:"ack_modes,omitempty"`
	RuntimeOwnership    string   `json:"runtime_ownership,omitempty"`
}

type CredentialSchema struct {
	Type                 string                     `json:"type"`
	LoginModes           []string                   `json:"login_modes"`
	Properties           map[string]CredentialField `json:"properties"`
	Required             []string                   `json:"required,omitempty"`
	AdditionalProperties bool                       `json:"additionalProperties"`
}

type CredentialField struct {
	Type        string `json:"type"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Secret      bool   `json:"secret,omitempty"`
}

type CredentialValidationRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid,omitempty"`
	ChannelUUID   string         `json:"channel_uuid,omitempty"`
	Platform      string         `json:"platform,omitempty"`
	Credential    map[string]any `json:"credential,omitempty"`
	State         map[string]any `json:"state,omitempty"`
	Runtime       Runtime        `json:"-"`
}

type CredentialValidationResult struct {
	Valid       bool           `json:"valid"`
	AccountKey  string         `json:"account_key,omitempty"`
	DisplayName string         `json:"display_name,omitempty"`
	Credential  map[string]any `json:"credential,omitempty"`
	State       map[string]any `json:"state,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Error       string         `json:"error,omitempty"`
}

// WebhookEndpoint describes the host-owned callback endpoint for one account.
// Platform credentials and verification secrets remain in ChannelAccount.Credential.
type WebhookEndpoint struct {
	URL string
}

type Runtime struct {
	WorkspaceUUID string
	Channel       Channel
	Account       ChannelAccount
	Accounts      []ChannelAccount
	// Webhook is optional and is populated by hosts for webhook-owned
	// runtimes. It is runtime configuration, not user credential.
	Webhook         *WebhookEndpoint
	Gateway         Gateway
	AccountStore    AccountStore
	Stream          StreamTransport
	HTTPClient      *http.Client
	Logger          *log.Logger
	PollInterval    time.Duration
	StreamReconnect time.Duration
	Native          any
}

// StreamTransport lets an SDK send a request over the host-owned stream that
// belongs to the current account. CorrelationID is opaque to the host; the SDK
// returns the matching value in StreamFrameResult.ResponseTo when the platform
// response arrives.
type StreamTransport interface {
	Request(ctx context.Context, req StreamRequest) (*StreamResponse, error)
}

type StreamRequest struct {
	Frame         StreamFrame
	CorrelationID string
	Timeout       time.Duration
}

type StreamResponse struct {
	Frame         StreamFrame
	CorrelationID string
}

type Gateway interface {
	EnsureChannel(ctx context.Context, req EnsureChannelRequest) (string, error)
	EnsureChannelLinkSession(ctx context.Context, req EnsureChannelLinkSessionRequest) (string, error)
	EnsureChatSession(ctx context.Context, req EnsureChatSessionRequest) (string, error)
	CreateMessage(ctx context.Context, req CreateMessageRequest) (string, error)
	StreamSession(ctx context.Context, req StreamSessionRequest, handle func(StreamEvent) error) error
	AgentParticipantID() string
	BridgeParticipantID(platform string) string
}

type AccountStore interface {
	LoadChannelAccountState(ctx context.Context, accountUUID string) (map[string]any, error)
	SaveChannelAccountState(ctx context.Context, accountUUID string, state map[string]any) error
}

type Channel struct {
	UUID          string         `json:"channel_uuid"`
	WorkspaceUUID string         `json:"workspace_uuid"`
	Platform      string         `json:"platform"`
	Name          string         `json:"name,omitempty"`
	Config        map[string]any `json:"config,omitempty"`
}

type ChannelAccount struct {
	UUID          string         `json:"account_uuid"`
	WorkspaceUUID string         `json:"workspace_uuid"`
	ChannelUUID   string         `json:"channel_uuid"`
	Platform      string         `json:"platform"`
	DisplayName   string         `json:"display_name,omitempty"`
	Credential    map[string]any `json:"credential,omitempty"`
	State         map[string]any `json:"state,omitempty"`
	Status        string         `json:"status,omitempty"`
}

type LoginStartRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid"`
	ChannelUUID   string         `json:"channel_uuid"`
	Payload       map[string]any `json:"payload,omitempty"`
	Runtime       Runtime        `json:"-"`
}

type LoginPollRequest struct {
	WorkspaceUUID  string         `json:"workspace_uuid"`
	ChannelUUID    string         `json:"channel_uuid"`
	ChallengeUUID  string         `json:"challenge_uuid,omitempty"`
	ChallengeCode  string         `json:"challenge_code,omitempty"`
	ChallengeState map[string]any `json:"challenge_state,omitempty"`
	Runtime        Runtime        `json:"-"`
}

type LoginChallenge struct {
	Type      string         `json:"type"`
	Code      string         `json:"code,omitempty"`
	URL       string         `json:"url,omitempty"`
	ExpiresAt *time.Time     `json:"expires_at,omitempty"`
	State     map[string]any `json:"state,omitempty"`
}

type LoginStatus struct {
	Status     string         `json:"status"`
	Confirmed  bool           `json:"confirmed"`
	Expired    bool           `json:"expired"`
	Account    ChannelAccount `json:"account,omitempty"`
	Credential map[string]any `json:"credential,omitempty"`
	State      map[string]any `json:"state,omitempty"`
}

type InboundMessage struct {
	WorkspaceUUID     string             `json:"workspace_uuid"`
	Platform          string             `json:"platform"`
	AccountUUID       string             `json:"account_uuid"`
	ChannelUUID       string             `json:"channel_uuid"`
	ChatType          string             `json:"chat_type"`
	ChatID            string             `json:"chat_id"`
	ThreadID          string             `json:"thread_id,omitempty"`
	ChatDisplayName   string             `json:"chat_display_name,omitempty"`
	ChatAvatarURL     string             `json:"chat_avatar_url,omitempty"`
	ChatIdentity      ChatIdentity       `json:"chat_identity,omitempty"`
	SenderID          string             `json:"sender_id"`
	SenderDisplayName string             `json:"sender_display_name,omitempty"`
	MessageID         string             `json:"message_id,omitempty"`
	Text              string             `json:"text"`
	ReferencedMessage *ReferencedMessage `json:"referenced_message,omitempty"`
	DedupeKey         string             `json:"dedupe_key,omitempty"`
	Mentions          []MentionIdentity  `json:"mentions,omitempty"`
	MentionedMe       bool               `json:"mentioned_me,omitempty"`
	MentionAll        bool               `json:"mention_all,omitempty"`
	Raw               map[string]any     `json:"raw,omitempty"`
}

type ReferencedMessage struct {
	Platform          string         `json:"platform,omitempty"`
	MessageID         string         `json:"message_id,omitempty"`
	ChatType          string         `json:"chat_type,omitempty"`
	ChatID            string         `json:"chat_id,omitempty"`
	ThreadID          string         `json:"thread_id,omitempty"`
	RootID            string         `json:"root_id,omitempty"`
	SenderID          string         `json:"sender_id,omitempty"`
	SenderDisplayName string         `json:"sender_display_name,omitempty"`
	MessageType       string         `json:"message_type,omitempty"`
	Text              string         `json:"text,omitempty"`
	CreatedAt         string         `json:"created_at,omitempty"`
	Raw               map[string]any `json:"raw,omitempty"`
}

type MentionIdentity struct {
	ID          string `json:"id"`
	IDType      string `json:"id_type,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type ChatIdentity struct {
	ID          string `json:"id,omitempty"`
	IDType      string `json:"id_type,omitempty"`
	Type        string `json:"type,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
}

type OutboundMessage struct {
	WorkspaceUUID string `json:"workspace_uuid"`
	Platform      string `json:"platform"`
	AccountUUID   string `json:"account_uuid"`
	ChannelUUID   string `json:"channel_uuid"`
	SessionUUID   string `json:"session_uuid"`
	MessageUUID   string `json:"message_uuid"`
	ChatType      string `json:"chat_type"`
	ChatID        string `json:"chat_id"`
	ThreadID      string `json:"thread_id,omitempty"`
	Text          string `json:"text"`
	// Format is a common host-facing field. Set "markdown" for rich rendering;
	// each SDK maps it to platform-native delivery or falls back internally.
	Format string `json:"format,omitempty"`
	// Title is a common markdown title hint. Hosts should pass it uniformly and
	// let the SDK decide whether the platform can use it.
	Title      string            `json:"title,omitempty"`
	Mentions   []MentionIdentity `json:"mentions,omitempty"`
	MentionAll bool              `json:"mention_all,omitempty"`
	// Raw is a platform-native escape hatch, not required for common text or markdown.
	Raw map[string]any `json:"raw,omitempty"`
}

type SendResult struct {
	Platform    string         `json:"platform"`
	AccountUUID string         `json:"account_uuid"`
	MessageID   string         `json:"message_id,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
}

type OutboundAck struct {
	WorkspaceUUID     string         `json:"workspace_uuid"`
	Platform          string         `json:"platform"`
	AccountUUID       string         `json:"account_uuid"`
	ChannelUUID       string         `json:"channel_uuid"`
	SessionUUID       string         `json:"session_uuid"`
	SourceMessageUUID string         `json:"source_message_uuid,omitempty"`
	ChatType          string         `json:"chat_type"`
	ChatID            string         `json:"chat_id"`
	TargetMessageID   string         `json:"target_message_id,omitempty"`
	Intent            string         `json:"intent,omitempty"`
	Action            string         `json:"action,omitempty"`
	Mode              string         `json:"mode,omitempty"`
	Emoji             string         `json:"emoji,omitempty"`
	Raw               map[string]any `json:"raw,omitempty"`
}

type AckResult struct {
	Platform    string         `json:"platform"`
	AccountUUID string         `json:"account_uuid"`
	Mode        string         `json:"mode,omitempty"`
	Status      string         `json:"status"`
	ReactionID  string         `json:"reaction_id,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
}

type EnsureChannelRequest struct {
	WorkspaceUUID string
	Platform      string
	Name          string
	Config        map[string]any
}

type EnsureChannelLinkSessionRequest struct {
	WorkspaceUUID       string
	Platform            string
	AccountUUID         string
	AgentParticipantID  string
	BridgeParticipantID string
}

type EnsureChatSessionRequest struct {
	WorkspaceUUID       string
	Platform            string
	AccountUUID         string
	ChatType            string
	ChatID              string
	ThreadID            string
	ChatDisplayName     string
	ChatAvatarURL       string
	ChatIdentity        ChatIdentity
	SenderID            string
	AgentParticipantID  string
	BridgeParticipantID string
	Metadata            map[string]any
}

type CreateMessageRequest struct {
	WorkspaceUUID string
	SessionUUID   string
	SenderID      string
	Content       string
	DedupeKey     string
	Metadata      map[string]any
}

type StreamSessionRequest struct {
	WorkspaceUUID string
	SessionUUID   string
	SubscriberID  string
	LastEventUUID string
	Filters       map[string]any
}

type StreamEvent struct {
	EventUUID     string          `json:"event_uuid,omitempty"`
	WorkspaceUUID string          `json:"workspace_uuid,omitempty"`
	SessionUUID   string          `json:"session_uuid,omitempty"`
	EventType     string          `json:"event_type,omitempty"`
	MessageUUID   string          `json:"message_uuid,omitempty"`
	SenderID      string          `json:"sender_id,omitempty"`
	Content       string          `json:"content,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}
