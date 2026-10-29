package sdk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// InteractionCapabilities is opt-in; zero values never advertise native approval.
type InteractionCapabilities struct {
	ToolApproval     bool   `json:"tool_approval"`
	PlanConfirmation bool   `json:"plan_confirmation"`
	Input            bool   `json:"input"`
	Update           string `json:"update,omitempty"` // always, callback, none
}

type InteractionButton struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type InteractionInput struct {
	ID       string              `json:"id"`
	Label    string              `json:"label"`
	Required bool                `json:"required,omitempty"`
	Options  []InteractionButton `json:"options,omitempty"`
}

// ToolApprovalDetails is display data from the host's persisted, redacted request.
// It must never be used to decide authorization or execute a tool.
type ToolApprovalDetails struct {
	ToolName           string `json:"tool_name"`
	ResolutionText     string `json:"resolution_text,omitempty"`
	RiskLevel          string `json:"risk_level,omitempty"`
	ArgumentsPreview   string `json:"arguments_preview,omitempty"`
	ArgumentsTruncated bool   `json:"arguments_truncated,omitempty"`
}

// PlanConfirmationDetails preserves structured human intent for presentation.
// State retains the released terminal values; decision_action distinguishes revise.
type PlanConfirmationDetails struct {
	FeedbackEnabled       bool   `json:"feedback_enabled,omitempty"`
	DecisionAction        string `json:"decision_action,omitempty"`
	Feedback              string `json:"feedback,omitempty"`
	AwaitingRevisionInput bool   `json:"awaiting_revision_input,omitempty"`
}

// InteractionCard carries a host-owned, persisted request. MessageID selects an
// update of a previously sent card. CallbackID is only used inside a platform's
// callback update window. Platform payloads must remain inside the connector.
type InteractionCard struct {
	// Language is the host-resolved reply language, fixed for this request.
	Language         string                   `json:"language,omitempty"`
	PlanConfirmation *PlanConfirmationDetails `json:"plan_confirmation,omitempty"`
	ToolApproval     *ToolApprovalDetails     `json:"tool_approval,omitempty"`
	TemplateID       string                   `json:"template_id,omitempty"`
	RequestID        string                   `json:"request_id"`
	Revision         int64                    `json:"revision"`
	Kind             string                   `json:"kind"`
	State            string                   `json:"state"`
	Title            string                   `json:"title"`
	Summary          string                   `json:"summary"`
	Actions          []InteractionButton      `json:"actions,omitempty"`
	Inputs           []InteractionInput       `json:"inputs,omitempty"`
	MessageID        string                   `json:"message_id,omitempty"`
	CallbackID       string                   `json:"callback_id,omitempty"`
	// Deprecated: WebURL is ignored. Decisions must stay on the originating IM.
	WebURL string `json:"web_url,omitempty"`
}

// ActorID and chat/message identity come from the verified platform envelope,
// never from button data. The host must authorize them against the request.
type InteractionAction struct {
	RequestID string            `json:"request_id"`
	Revision  int64             `json:"revision"`
	ActionID  string            `json:"action_id"`
	ActorID   string            `json:"actor_id"`
	ChatID    string            `json:"chat_id"`
	MessageID string            `json:"message_id"`
	EventID   string            `json:"event_id"`
	Inputs    map[string]string `json:"inputs,omitempty"`
}

type InteractionResult struct {
	State string           `json:"state"`
	Card  *InteractionCard `json:"card,omitempty"`
}

// InteractionGateway is an optional host extension. Old gateways keep compiling
// and connectors must return a clear error when native actions are not supported.
type InteractionGateway interface {
	HandleInteraction(context.Context, InteractionAction) (*InteractionResult, error)
}

// CallbackInteractionGateway coordinates a callback update with the host's
// independently published terminal notification. Complete must be called once.
type CallbackInteractionGateway interface {
	BeginCallbackInteraction(context.Context, InteractionAction) (*InteractionResult, func(bool), error)
}

func DispatchInteraction(ctx context.Context, runtime Runtime, action InteractionAction) (*InteractionResult, error) {
	gateway, ok := runtime.Gateway.(InteractionGateway)
	if !ok {
		return nil, fmt.Errorf("interaction host unavailable")
	}
	if action.ActorID == "" || action.ChatID == "" || action.EventID == "" {
		return nil, fmt.Errorf("interaction platform identity missing")
	}
	if _, err := action.Token(); err != nil {
		return nil, err
	}
	return gateway.HandleInteraction(ctx, action)
}

// Token fits Telegram's 64-byte callback_data limit. It is a request reference,
// not a bearer authorization; tenant, account, actor and revision are checked by
// the host after SDK authentication of the platform envelope.
func (a InteractionAction) Token() (string, error) {
	if a.RequestID == "" || a.Revision < 0 {
		return "", fmt.Errorf("invalid interaction identity")
	}
	for _, c := range a.RequestID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return "", fmt.Errorf("invalid interaction identity")
		}
	}
	switch a.ActionID {
	case "approve", "reject", "revise", "submit":
	default:
		return "", fmt.Errorf("unsupported interaction action")
	}
	value := a.RequestID + ":" + strconv.FormatInt(a.Revision, 36) + ":" + a.ActionID
	if len(value) > 64 {
		return "", fmt.Errorf("interaction reference exceeds 64 bytes")
	}
	return value, nil
}

func ParseInteractionToken(value string) (InteractionAction, error) {
	var action InteractionAction
	parts := strings.Split(value, ":")
	if len(parts) != 3 {
		return action, fmt.Errorf("invalid interaction reference")
	}
	revision, err := strconv.ParseInt(parts[1], 36, 64)
	if err != nil {
		return action, fmt.Errorf("invalid interaction revision")
	}
	action = InteractionAction{RequestID: parts[0], Revision: revision, ActionID: parts[2]}
	token, err := action.Token()
	if err != nil || token != value {
		return InteractionAction{}, fmt.Errorf("invalid interaction reference")
	}
	return action, nil
}

func (c InteractionCard) Validate() error {
	if _, err := (InteractionAction{RequestID: c.RequestID, Revision: c.Revision, ActionID: "approve"}).Token(); err != nil {
		return err
	}
	switch c.Kind {
	case "tool_approval", "plan_confirmation", "input_request":
	default:
		return fmt.Errorf("unsupported interaction kind")
	}
	switch c.State {
	case "pending", "approved", "rejected", "canceled", "expired":
	default:
		return fmt.Errorf("unsupported interaction state")
	}
	if c.State != "pending" && (len(c.Actions) > 0 || len(c.Inputs) > 0) {
		return fmt.Errorf("terminal interaction must not be actionable")
	}
	if len(c.Actions) > 5 || len(c.Inputs) > 5 {
		return fmt.Errorf("interaction exceeds native card limits")
	}
	seen := map[string]bool{}
	for _, button := range c.Actions {
		if button.Label == "" || seen[button.ID] {
			return fmt.Errorf("invalid interaction button")
		}
		if _, err := (InteractionAction{RequestID: c.RequestID, Revision: c.Revision, ActionID: button.ID}).Token(); err != nil {
			return err
		}
		seen[button.ID] = true
	}
	fields := map[string]bool{}
	for _, field := range c.Inputs {
		if field.ID == "" || field.ID == "beak_action" || field.ID == "action" || field.Label == "" || fields[field.ID] || len(field.Options) > 100 {
			return fmt.Errorf("invalid interaction input")
		}
		fields[field.ID] = true
		choices := map[string]bool{}
		for _, option := range field.Options {
			if option.ID == "" || option.Label == "" || choices[option.ID] {
				return fmt.Errorf("invalid interaction choice")
			}
			choices[option.ID] = true
		}
	}

	return nil
}

func (c InteractionCard) ButtonToken(button InteractionButton) (string, error) {
	return (InteractionAction{RequestID: c.RequestID, Revision: c.Revision, ActionID: button.ID}).Token()
}

// ResolveInteractionLanguage accepts supported language tags and otherwise uses
// the platform default. It never detects language from user or model content.
func ResolveInteractionLanguage(platform, language string) string {
	language = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(language, "_", "-")))
	switch strings.SplitN(language, "-", 2)[0] {
	case "zh":
		return "zh"
	case "en":
		return "en"
	}
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "feishu", "dingtalk", "wecom", "weixin", "wechat":
		return "zh"
	default:
		return "en"
	}
}

// InteractionErrorLanguage reads only trusted host error metadata. Callback
// payloads cannot choose the language of another member's approval request.
func InteractionErrorLanguage(platform string, err error) string {
	var localized interface{ InteractionLanguage() string }
	if errors.As(err, &localized) {
		return ResolveInteractionLanguage(platform, localized.InteractionLanguage())
	}
	return ResolveInteractionLanguage(platform, "")
}
