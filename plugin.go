package beakwecom

import "context"

const (
	ID       = "beak-agent-wecom"
	Platform = "wecom"
)

type API interface {
	RegisterChannel(Channel) error
}

type Plugin struct{}

type Channel struct{}

type Metadata struct {
	ID          string
	Platform    string
	Label       string
	Description string
}

type Capabilities struct {
	DirectChat     bool
	GroupChat      bool
	Text           bool
	Media          bool
	BlockStreaming bool
}

type SettingsSchema struct {
	Type                 string         `json:"type"`
	AdditionalProperties bool           `json:"additionalProperties"`
	Properties           map[string]any `json:"properties"`
	Required             []string       `json:"required,omitempty"`
}

func New() Plugin { return Plugin{} }

func Register(api API) error { return New().Register(api) }

func (Plugin) Register(api API) error { return api.RegisterChannel(Channel{}) }

func (Plugin) Channel() Channel { return Channel{} }

func (Channel) Metadata() Metadata {
	return Metadata{
		ID:          ID,
		Platform:    Platform,
		Label:       "企业微信",
		Description: "企业微信智能机器人长连接 Channel SDK",
	}
}

func (Channel) Capabilities() Capabilities {
	return Capabilities{
		DirectChat:     true,
		GroupChat:      true,
		Text:           true,
		Media:          false,
		BlockStreaming: false,
	}
}

func (Channel) SettingsSchema() SettingsSchema {
	return SettingsSchema{
		Type:                 "object",
		AdditionalProperties: false,
		Required:             []string{"bot_id", "secret"},
		Properties: map[string]any{
			"bot_id": map[string]any{
				"type":        "string",
				"title":       "Bot ID",
				"description": "企业微信智能机器人的 Bot ID。",
			},
			"secret": map[string]any{
				"type":        "string",
				"title":       "Secret",
				"description": "企业微信智能机器人的 Secret。",
				"secret":      true,
			},
		},
	}
}

func (Channel) CheckHealth(context.Context) error { return nil }
