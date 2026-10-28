package beakwecom

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	maxTrackedOutboundProgress  = 256
	maxWecomMediaBytes          = 50 << 20
	wecomMediaChunkBytes        = 512 << 10
	outboundChunkProgressKey    = "outbound_chunk_progress"
)

var outboundSendLocks [64]sync.Mutex

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
			Interactions:     sdk.InteractionCapabilities{ToolApproval: true, PlanConfirmation: true, Update: "callback"},
			LoginModes:       []string{sdk.LoginModeCredential},
			Text:             caps.Text,
			Media:            caps.Media,
			MediaKinds:       append([]string(nil), caps.MediaKinds...),
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

func (c Connector) Send(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	accountUUID := accountKey(account)
	if strings.TrimSpace(req.ChatID) == "" {
		return nil, fmt.Errorf("wecom outbound chat_id is required")
	}
	if req.Interaction != nil {
		return sendWecomInteraction(ctx, runtime, req)
	}
	if strings.TrimSpace(req.Text) == "" && len(req.Attachments) == 0 {
		return nil, fmt.Errorf("wecom outbound text or attachments are required")
	}
	if runtime.Stream == nil {
		return nil, fmt.Errorf("wecom host stream transport is unavailable")
	}
	if len(req.Attachments) > 0 {
		result := &sdk.SendResult{Platform: Platform, AccountUUID: accountUUID, Raw: map[string]any{"delivery_method": "host_stream"}}
		if strings.TrimSpace(req.Text) != "" {
			textReq := req
			textReq.Attachments = nil
			textResult, textErr := c.Send(ctx, runtime, textReq)
			if textErr != nil {
				return nil, textErr
			}
			result.MessageID = textResult.MessageID
			if textResult.Raw != nil {
				result.Raw["text_result"] = textResult.Raw
			}
		}
		var attachmentErrors []string
		for _, attachment := range req.Attachments {
			item := sdk.AttachmentSendResult{AttachmentID: attachment.ID, Kind: attachment.Kind, Status: sdk.AttachmentStatusFailed}
			if validateErr := sdk.ValidateMediaAttachment(attachment); validateErr != nil {
				item.Error = validateErr.Error()
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			mediaPath := strings.TrimSpace(attachment.Path)
			if mediaPath == "" {
				item.Error = "wecom outbound attachments require a local path"
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			data, readErr := readWecomOutboundMedia(mediaPath)
			if readErr != nil {
				item.Error = readErr.Error()
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			fileName := strings.TrimSpace(attachment.FileName)
			if fileName == "" {
				fileName = filepath.Base(mediaPath)
			}
			mediaID, uploadErr := uploadWecomMedia(ctx, runtime.Stream, attachment.Kind, fileName, data)
			if uploadErr != nil {
				item.Error = uploadErr.Error()
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			requestID, requestErr := platform.NewRequestID(platform.CommandSendMessage)
			if requestErr != nil {
				item.Error = requestErr.Error()
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			payload, marshalErr := platform.MarshalFrame(platform.SendMediaFrame(requestID, req.ChatID, attachment.Kind, mediaID))
			if marshalErr != nil {
				item.Error = marshalErr.Error()
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			response, sendErr := runtime.Stream.Request(ctx, sdk.StreamRequest{Frame: sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload}, CorrelationID: requestID, Timeout: streamRequestTimeout})
			if sendErr != nil || response == nil {
				if sendErr == nil {
					sendErr = fmt.Errorf("empty stream response")
				}
				item.Error = sendErr.Error()
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			ack, parseErr := platform.ParseFrame(response.Frame.Data)
			if parseErr != nil || ack.ErrCode != 0 {
				if parseErr != nil {
					item.Error = parseErr.Error()
				} else {
					item.Error = fmt.Sprintf("wecom media send failed: code=%d message=%s", ack.ErrCode, ack.ErrMsg)
				}
				attachmentErrors = append(attachmentErrors, item.Error)
				result.AttachmentResults = append(result.AttachmentResults, item)
				continue
			}
			item.Status = sdk.AttachmentStatusSent
			item.MessageID = firstString(stringValue(ack.Body["msgid"]), requestID)
			item.PlatformResourceID = mediaID
			result.MessageID = item.MessageID
			result.AttachmentResults = append(result.AttachmentResults, item)
		}
		if len(attachmentErrors) > 0 {
			result.Raw["attachment_errors"] = attachmentErrors
		}
		if result.MessageID == "" {
			return result, fmt.Errorf("%s outbound attachments failed", Platform)
		}
		return result, nil
	}

	chunks := splitUTF8(req.Text, platform.MaxMarkdownBytes)
	if len(chunks) > 1 && strings.TrimSpace(req.MessageUUID) == "" {
		return nil, fmt.Errorf("wecom multipart outbound requires message_uuid for retry-safe delivery")
	}
	if len(chunks) > 1 && runtime.AccountStore == nil {
		return nil, fmt.Errorf("wecom multipart outbound requires sdk.Runtime.AccountStore for retry-safe delivery")
	}
	progressEntries := map[string]any{}
	progress := outboundChunkProgress{}
	if len(chunks) > 1 {
		lock := outboundSendLock(accountUUID)
		lock.Lock()
		defer lock.Unlock()

		progressEntries, progress, err = loadOutboundChunkProgress(ctx, runtime, account, req, chunks)
		if err != nil {
			return nil, err
		}
		if progress.Completed {
			return wecomSendResult(accountUUID, progress.MessageIDs, len(chunks)), nil
		}
	}
	messageIDs := append([]string(nil), progress.MessageIDs...)
	for index := progress.NextIndex; index < len(chunks); index++ {
		chunk := chunks[index]
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
		if progress.Key != "" {
			progress.NextIndex = index + 1
			progress.MessageIDs = append([]string(nil), messageIDs...)
			progress.Completed = progress.NextIndex == len(chunks)
			progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			progressEntries[progress.Key] = encodeOutboundChunkProgress(progress)
			pruneOutboundChunkProgress(progressEntries, maxTrackedOutboundProgress)
			patch := map[string]any{outboundChunkProgressKey: encodeOutboundChunkProgressEntries(progressEntries)}
			if err := saveAccountState(ctx, runtime, accountUUID, patch); err != nil {
				return nil, fmt.Errorf("wecom persist send progress after chunk %d/%d: %w", index+1, len(chunks), err)
			}
		}
	}
	return wecomSendResult(accountUUID, messageIDs, len(chunks)), nil
}

func readWecomOutboundMedia(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("wecom media path is not a regular file")
	}
	if info.Size() > maxWecomMediaBytes {
		return nil, fmt.Errorf("wecom media exceeds %d bytes", maxWecomMediaBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxWecomMediaBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxWecomMediaBytes {
		return nil, fmt.Errorf("wecom media exceeds %d bytes", maxWecomMediaBytes)
	}
	return data, nil
}

func uploadWecomMedia(ctx context.Context, stream sdk.StreamTransport, kind, fileName string, data []byte) (string, error) {
	if stream == nil {
		return "", fmt.Errorf("wecom host stream transport is unavailable")
	}
	kind = strings.TrimSpace(kind)
	if kind == "sticker" {
		kind = sdk.MediaKindImage
	}
	if kind != sdk.MediaKindImage && kind != sdk.MediaKindFile && kind != sdk.MediaKindAudio && kind != sdk.MediaKindVideo {
		return "", fmt.Errorf("wecom outbound media kind %q is unsupported", kind)
	}
	if fileName == "" {
		fileName = "attachment"
	}
	chunkCount := (len(data) + wecomMediaChunkBytes - 1) / wecomMediaChunkBytes
	if chunkCount == 0 {
		chunkCount = 1
	}
	initID, err := platform.NewRequestID(platform.CommandUploadInit)
	if err != nil {
		return "", err
	}
	initFrame, err := platform.MarshalFrame(platform.UploadMediaInitFrame(initID, kind, fileName, int64(len(data)), chunkCount, fmt.Sprintf("%x", md5.Sum(data))))
	if err != nil {
		return "", err
	}
	initAck, err := requestWecomUploadFrame(ctx, stream, initID, initFrame)
	if err != nil {
		return "", fmt.Errorf("wecom media upload init: %w", err)
	}
	uploadID := stringValue(initAck.Body["upload_id"])
	if uploadID == "" {
		return "", fmt.Errorf("wecom media upload init returned no upload_id")
	}
	for index, start := 0, 0; start < len(data); index, start = index+1, start+wecomMediaChunkBytes {
		end := start + wecomMediaChunkBytes
		if end > len(data) {
			end = len(data)
		}
		chunkID, err := platform.NewRequestID(platform.CommandUploadChunk)
		if err != nil {
			return "", err
		}
		chunkFrame, err := platform.MarshalFrame(platform.UploadMediaChunkFrame(chunkID, uploadID, index+1, base64.StdEncoding.EncodeToString(data[start:end])))
		if err != nil {
			return "", err
		}
		if _, err := requestWecomUploadFrame(ctx, stream, chunkID, chunkFrame); err != nil {
			return "", fmt.Errorf("wecom media upload chunk %d/%d: %w", index+1, chunkCount, err)
		}
	}
	finishID, err := platform.NewRequestID(platform.CommandUploadFinish)
	if err != nil {
		return "", err
	}
	finishFrame, err := platform.MarshalFrame(platform.UploadMediaFinishFrame(finishID, uploadID))
	if err != nil {
		return "", err
	}
	finishAck, err := requestWecomUploadFrame(ctx, stream, finishID, finishFrame)
	if err != nil {
		return "", fmt.Errorf("wecom media upload finish: %w", err)
	}
	mediaID := stringValue(finishAck.Body["media_id"])
	if mediaID == "" {
		return "", fmt.Errorf("wecom media upload finish returned no media_id")
	}
	return mediaID, nil
}

func requestWecomUploadFrame(ctx context.Context, stream sdk.StreamTransport, requestID string, payload []byte) (platform.Frame, error) {
	response, err := stream.Request(ctx, sdk.StreamRequest{
		Frame:         sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: payload},
		CorrelationID: requestID,
		Timeout:       streamRequestTimeout,
	})
	if err != nil {
		return platform.Frame{}, err
	}
	if response == nil {
		return platform.Frame{}, fmt.Errorf("empty stream response")
	}
	ack, err := platform.ParseFrame(response.Frame.Data)
	if err != nil {
		return platform.Frame{}, err
	}
	if ack.Headers.RequestID != requestID {
		return platform.Frame{}, fmt.Errorf("response req_id mismatch: got %q want %q", ack.Headers.RequestID, requestID)
	}
	if ack.ErrCode != 0 {
		return platform.Frame{}, fmt.Errorf("code=%d message=%s", ack.ErrCode, strings.TrimSpace(ack.ErrMsg))
	}
	return ack, nil
}

func wecomSendResult(accountUUID string, messageIDs []string, chunkCount int) *sdk.SendResult {
	result := &sdk.SendResult{
		Platform:    Platform,
		AccountUUID: accountUUID,
		Raw: map[string]any{
			"message_ids":     append([]string(nil), messageIDs...),
			"delivery_method": "host_stream",
			"msg_type":        "markdown",
			"chunk_count":     chunkCount,
		},
	}
	if len(messageIDs) > 0 {
		result.MessageID = messageIDs[len(messageIDs)-1]
	}
	return result
}

type outboundChunkProgress struct {
	Key         string
	Fingerprint string
	NextIndex   int
	MessageIDs  []string
	Completed   bool
	UpdatedAt   string
}

func loadOutboundChunkProgress(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req sdk.OutboundMessage, chunks []string) (map[string]any, outboundChunkProgress, error) {
	progress := outboundChunkProgress{Key: strings.TrimSpace(req.MessageUUID), Fingerprint: outboundMessageFingerprint(accountKey(account), req, chunks)}
	state, err := loadAccountState(ctx, runtime, account)
	if err != nil {
		return nil, outboundChunkProgress{}, err
	}
	entries := decodeOutboundChunkProgressEntries(state[outboundChunkProgressKey])
	storedValue, exists := entries[progress.Key]
	if !exists {
		return entries, progress, nil
	}
	stored := decodeOutboundChunkProgress(progress.Key, storedValue)
	if stored.Fingerprint != progress.Fingerprint {
		return nil, outboundChunkProgress{}, fmt.Errorf("wecom message_uuid %s was already used for a different outbound payload", progress.Key)
	}
	if stored.NextIndex < 0 || stored.NextIndex > len(chunks) || len(stored.MessageIDs) != stored.NextIndex {
		return nil, outboundChunkProgress{}, fmt.Errorf("wecom outbound progress for message_uuid %s is invalid", progress.Key)
	}
	stored.Key = progress.Key
	stored.Completed = stored.NextIndex == len(chunks) && len(stored.MessageIDs) > 0
	return entries, stored, nil
}

func outboundMessageFingerprint(accountUUID string, req sdk.OutboundMessage, chunks []string) string {
	value, _ := json.Marshal(struct {
		AccountUUID string   `json:"account_uuid"`
		ChatType    string   `json:"chat_type"`
		ChatID      string   `json:"chat_id"`
		Chunks      []string `json:"chunks"`
	}{
		AccountUUID: strings.TrimSpace(accountUUID),
		ChatType:    strings.TrimSpace(req.ChatType),
		ChatID:      strings.TrimSpace(req.ChatID),
		Chunks:      chunks,
	})
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func encodeOutboundChunkProgress(progress outboundChunkProgress) map[string]any {
	messageIDs := make([]any, 0, len(progress.MessageIDs))
	for _, messageID := range progress.MessageIDs {
		messageIDs = append(messageIDs, messageID)
	}
	return map[string]any{
		"fingerprint": progress.Fingerprint,
		"next_index":  progress.NextIndex,
		"message_ids": messageIDs,
		"completed":   progress.Completed,
		"updated_at":  progress.UpdatedAt,
	}
}

func decodeOutboundChunkProgress(key string, value any) outboundChunkProgress {
	raw := mapValue(value)
	progress := outboundChunkProgress{
		Key:         key,
		Fingerprint: strings.TrimSpace(stringValue(raw["fingerprint"])),
		NextIndex:   intValue(raw["next_index"]),
		Completed:   boolValue(raw["completed"]),
		UpdatedAt:   strings.TrimSpace(stringValue(raw["updated_at"])),
	}
	switch values := raw["message_ids"].(type) {
	case []any:
		for _, value := range values {
			if messageID := strings.TrimSpace(stringValue(value)); messageID != "" {
				progress.MessageIDs = append(progress.MessageIDs, messageID)
			}
		}
	case []string:
		for _, value := range values {
			if messageID := strings.TrimSpace(value); messageID != "" {
				progress.MessageIDs = append(progress.MessageIDs, messageID)
			}
		}
	}
	return progress
}

func decodeOutboundChunkProgressEntries(value any) map[string]any {
	entries := map[string]any{}
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			entries[key] = item
		}
	case map[string]string:
		for key, item := range typed {
			entries[key] = item
		}
	case []any:
		for _, item := range typed {
			raw := mapValue(item)
			key := strings.TrimSpace(stringValue(raw["message_uuid"]))
			if key == "" {
				continue
			}
			delete(raw, "message_uuid")
			entries[key] = raw
		}
	case []map[string]any:
		for _, item := range typed {
			raw := cloneMap(item)
			key := strings.TrimSpace(stringValue(raw["message_uuid"]))
			if key == "" {
				continue
			}
			delete(raw, "message_uuid")
			entries[key] = raw
		}
	}
	return entries
}

func encodeOutboundChunkProgressEntries(entries map[string]any) []any {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded := make([]any, 0, len(keys))
	for _, key := range keys {
		entry := mapValue(entries[key])
		entry["message_uuid"] = key
		encoded = append(encoded, entry)
	}
	return encoded
}

func pruneOutboundChunkProgress(entries map[string]any, limit int) {
	if limit <= 0 || len(entries) <= limit {
		return
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return stringValue(mapValue(entries[keys[i]])["updated_at"]) < stringValue(mapValue(entries[keys[j]])["updated_at"])
	})
	for len(entries) > limit {
		delete(entries, keys[0])
		keys = keys[1:]
	}
}

func outboundSendLock(accountUUID string) *sync.Mutex {
	sum := sha256.Sum256([]byte(strings.TrimSpace(accountUUID)))
	return &outboundSendLocks[int(sum[0])%len(outboundSendLocks)]
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

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
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
