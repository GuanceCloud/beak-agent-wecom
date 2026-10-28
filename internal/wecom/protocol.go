package wecom

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	DefaultWebSocketURL = "wss://openws.work.weixin.qq.com"

	CommandSubscribe    = "aibot_subscribe"
	CommandPing         = "ping"
	CommandRespond      = "aibot_respond_msg"
	CommandSendMessage  = "aibot_send_msg"
	CommandUploadInit   = "aibot_upload_media_init"
	CommandUploadChunk  = "aibot_upload_media_chunk"
	CommandUploadFinish = "aibot_upload_media_finish"
	CommandMessage      = "aibot_msg_callback"
	CommandEvent        = "aibot_event_callback"
	EventDisconnected   = "disconnected_event"
	MessageTypeText     = "text"
	MessageTypeImage    = "image"
	MessageTypeMixed    = "mixed"
	MessageTypeVoice    = "voice"
	MessageTypeFile     = "file"
	MessageTypeVideo    = "video"
	MessageTypeEvent    = "event"
	ChatTypeSingle      = "single"
	ChatTypeGroup       = "group"
	MaxMarkdownBytes    = 20480
)

type Frame struct {
	Command string         `json:"cmd,omitempty"`
	Headers FrameHeaders   `json:"headers"`
	Body    map[string]any `json:"body,omitempty"`
	ErrCode int            `json:"errcode,omitempty"`
	ErrMsg  string         `json:"errmsg,omitempty"`
}

type FrameHeaders struct {
	RequestID string `json:"req_id"`
}

type Message struct {
	MessageID   string        `json:"msgid"`
	BotID       string        `json:"aibotid"`
	ChatID      string        `json:"chatid,omitempty"`
	ChatType    string        `json:"chattype"`
	From        MessageFrom   `json:"from"`
	CreateTime  int64         `json:"create_time,omitempty"`
	ResponseURL string        `json:"response_url,omitempty"`
	MessageType string        `json:"msgtype"`
	Text        *TextContent  `json:"text,omitempty"`
	Image       *MediaContent `json:"image,omitempty"`
	Mixed       *MixedContent `json:"mixed,omitempty"`
	Voice       *TextContent  `json:"voice,omitempty"`
	File        *MediaContent `json:"file,omitempty"`
	Video       *MediaContent `json:"video,omitempty"`
	Quote       *QuoteContent `json:"quote,omitempty"`
	Event       MessageEvent  `json:"event,omitempty"`
}

// MessageEvent owns the structured event payload. Card fields are normalized
// once at the platform boundary, rather than passed as untyped nested maps.
type MessageEvent struct {
	EventType         string             `json:"eventtype"`
	TemplateCardEvent *TemplateCardEvent `json:"template_card_event,omitempty"`
	EventKey          string             `json:"event_key,omitempty"`
	TaskID            string             `json:"task_id,omitempty"`
}

type TemplateCardEvent struct {
	EventKey string `json:"event_key"`
	TaskID   string `json:"task_id"`
}

func (e MessageEvent) CardAction() (TemplateCardEvent, error) {
	if e.TemplateCardEvent != nil {
		card := *e.TemplateCardEvent
		if (e.EventKey != "" && e.EventKey != card.EventKey) || (e.TaskID != "" && e.TaskID != card.TaskID) {
			return TemplateCardEvent{}, fmt.Errorf("wecom conflicting template card fields")
		}
		return card, nil
	}
	return TemplateCardEvent{EventKey: e.EventKey, TaskID: e.TaskID}, nil
}

type MessageFrom struct {
	UserID string `json:"userid"`
	CorpID string `json:"corpid,omitempty"`
}

type TextContent struct {
	Content string `json:"content"`
}

type MediaContent struct {
	URL    string `json:"url,omitempty"`
	AESKey string `json:"aeskey,omitempty"`
}

type MixedItem struct {
	MessageType string        `json:"msgtype"`
	Text        *TextContent  `json:"text,omitempty"`
	Image       *MediaContent `json:"image,omitempty"`
}

type MixedContent struct {
	Items []MixedItem `json:"msg_item"`
}

type QuoteContent struct {
	MessageType string        `json:"msgtype"`
	Text        *TextContent  `json:"text,omitempty"`
	Image       *MediaContent `json:"image,omitempty"`
	Mixed       *MixedContent `json:"mixed,omitempty"`
	Voice       *TextContent  `json:"voice,omitempty"`
	File        *MediaContent `json:"file,omitempty"`
	Video       *MediaContent `json:"video,omitempty"`
}

func NewRequestID(prefix string) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "request"
	}
	return prefix + "_" + hex.EncodeToString(raw), nil
}

func MarshalFrame(frame Frame) ([]byte, error) {
	if strings.TrimSpace(frame.Headers.RequestID) == "" {
		return nil, fmt.Errorf("wecom frame req_id is required")
	}
	return json.Marshal(frame)
}

func ParseFrame(data []byte) (Frame, error) {
	var frame Frame
	if err := json.Unmarshal(data, &frame); err != nil {
		return Frame{}, fmt.Errorf("decode wecom stream frame: %w", err)
	}
	if strings.TrimSpace(frame.Headers.RequestID) == "" {
		return Frame{}, fmt.Errorf("wecom stream frame req_id is required")
	}
	return frame, nil
}

func DecodeMessage(body map[string]any) (Message, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return Message{}, err
	}
	var message Message
	if err := json.Unmarshal(raw, &message); err != nil {
		return Message{}, fmt.Errorf("decode wecom callback message: %w", err)
	}
	return message, nil
}

func AuthFrame(requestID, botID, secret string) Frame {
	return Frame{
		Command: CommandSubscribe,
		Headers: FrameHeaders{RequestID: requestID},
		Body: map[string]any{
			"bot_id": strings.TrimSpace(botID),
			"secret": strings.TrimSpace(secret),
		},
	}
}

func PingFrame(requestID string) Frame {
	return Frame{Command: CommandPing, Headers: FrameHeaders{RequestID: requestID}}
}

func SendMarkdownFrame(requestID, chatID, content string) Frame {
	return Frame{
		Command: CommandSendMessage,
		Headers: FrameHeaders{RequestID: requestID},
		Body: map[string]any{
			"chatid":   strings.TrimSpace(chatID),
			"msgtype":  "markdown",
			"markdown": map[string]any{"content": content},
		},
	}
}

func SendMediaFrame(requestID, chatID, kind, mediaID string) Frame {
	kind = strings.TrimSpace(kind)
	msgType := kind
	if kind == "sticker" {
		msgType = MessageTypeImage
	}
	content := map[string]any{"media_id": strings.TrimSpace(mediaID)}
	return Frame{
		Command: CommandSendMessage,
		Headers: FrameHeaders{RequestID: requestID},
		Body: map[string]any{
			"chatid":  strings.TrimSpace(chatID),
			"msgtype": msgType,
			msgType:   content,
		},
	}
}

func UploadMediaInitFrame(requestID, kind, fileName string, size int64, chunks int, md5sum string) Frame {
	return Frame{
		Command: CommandUploadInit,
		Headers: FrameHeaders{RequestID: requestID},
		Body: map[string]any{
			"type":         strings.TrimSpace(kind),
			"filename":     strings.TrimSpace(fileName),
			"total_size":   size,
			"total_chunks": chunks,
			"md5":          strings.TrimSpace(md5sum),
		},
	}
}

func UploadMediaChunkFrame(requestID, uploadID string, index int, encoded string) Frame {
	return Frame{
		Command: CommandUploadChunk,
		Headers: FrameHeaders{RequestID: requestID},
		Body: map[string]any{
			"upload_id":   strings.TrimSpace(uploadID),
			"chunk_index": index,
			"base64_data": encoded,
		},
	}
}

func UploadMediaFinishFrame(requestID, uploadID string) Frame {
	return Frame{
		Command: CommandUploadFinish,
		Headers: FrameHeaders{RequestID: requestID},
		Body:    map[string]any{"upload_id": strings.TrimSpace(uploadID)},
	}
}

func MessageText(message Message) string {
	switch strings.TrimSpace(message.MessageType) {
	case MessageTypeText:
		return textValue(message.Text)
	case MessageTypeVoice:
		return textValue(message.Voice)
	case MessageTypeMixed:
		return MixedText(message.Mixed)
	case MessageTypeImage:
		return "[图片]"
	case MessageTypeFile:
		return "[文件]"
	case MessageTypeVideo:
		return "[视频]"
	default:
		return ""
	}
}

func QuoteText(quote *QuoteContent) string {
	if quote == nil {
		return ""
	}
	switch strings.TrimSpace(quote.MessageType) {
	case MessageTypeText:
		return textValue(quote.Text)
	case MessageTypeVoice:
		return textValue(quote.Voice)
	case MessageTypeMixed:
		return MixedText(quote.Mixed)
	case MessageTypeImage:
		return "[图片]"
	case MessageTypeFile:
		return "[文件]"
	case MessageTypeVideo:
		return "[视频]"
	default:
		return ""
	}
}

func MixedText(mixed *MixedContent) string {
	if mixed == nil {
		return ""
	}
	parts := make([]string, 0, len(mixed.Items))
	for _, item := range mixed.Items {
		switch strings.TrimSpace(item.MessageType) {
		case MessageTypeText:
			if value := strings.TrimSpace(textValue(item.Text)); value != "" {
				parts = append(parts, value)
			}
		case MessageTypeImage:
			parts = append(parts, "[图片]")
		}
	}
	return strings.Join(parts, "\n")
}

func textValue(content *TextContent) string {
	if content == nil {
		return ""
	}
	return strings.TrimSpace(content.Content)
}
