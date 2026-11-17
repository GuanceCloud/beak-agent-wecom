package beakwecom

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

// DisplayNames is a read-only snapshot of names visible to one bot credential.
type DisplayNames struct {
	BotName       string
	conversations map[conversationIdentity]string
}

type conversationIdentity struct {
	kind string
	id   string
}

// ConversationName uses the same ID and chat kind as the inbound callback.
// An encrypted CLI contact ID cannot be matched to a callback by name.
func (n DisplayNames) ConversationName(chatType, chatID string) string {
	return n.conversations[conversationIdentity{kind: chatType, id: chatID}]
}

// LookupDisplayNames queries optional display metadata outside the message path.
// Each result is independent so a denied query does not discard other names.
func (Connector) LookupDisplayNames(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount) DisplayNames {
	botID := strings.TrimSpace(stringValue(account.Credential["bot_id"]))
	secret := strings.TrimSpace(stringValue(account.Credential["secret"]))
	if botID == "" || secret == "" {
		return DisplayNames{}
	}
	client := http.Client{}
	if runtime.HTTPClient != nil {
		client = *runtime.HTTPClient
	}
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	now := time.Now()
	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		return DisplayNames{}
	}
	nonce := "cli_" + strconv.FormatInt(now.UnixMilli(), 10) + "_" + hex.EncodeToString(random[:])
	signature := sha256.Sum256([]byte(secret + botID + strconv.FormatInt(now.Unix(), 10) + nonce))
	auth := map[string]any{
		"bot_id": botID, "time": now.Unix(), "nonce": nonce,
		"signature": hex.EncodeToString(signature[:]), "bind_source": 1,
	}
	token := wecomNameRequest(ctx, &client, "/cgi-bin/aibot/cli/get_cli_config", "", auth).Token
	if token == "" {
		return DisplayNames{}
	}

	var names DisplayNames
	var identity struct {
		Context string `json:"extra_identity_context"`
	}
	if wecomCLINameResult(ctx, &client, "/cli/identity/whoami", token, &identity) {
		names.BotName = wecomBotName(identity.Context)
	}
	var result struct {
		Sessions []struct {
			ID   string `json:"chat_id"`
			Kind string `json:"chat_type"`
			Name string `json:"chat_name"`
		} `json:"sessions"`
	}
	if wecomCLINameResult(ctx, &client, "/cli/message/aibot/sessions/list", token, &result) {
		names.conversations = make(map[conversationIdentity]string, len(result.Sessions))
		for _, session := range result.Sessions {
			kind := session.Kind
			switch kind {
			case "single":
				kind = sdk.ChatTypeDirect
			case "group":
				kind = sdk.ChatTypeGroup
			default:
				continue
			}
			if name := validWeComName(session.Name); session.ID != "" && name != "" {
				names.conversations[conversationIdentity{kind: kind, id: session.ID}] = name
			}
		}
	}
	return names
}

type wecomNameResponse struct {
	Code    *int   `json:"errcode"`
	Token   string `json:"token"`
	Results string `json:"results_json"`
}

func wecomNameRequest(ctx context.Context, client *http.Client, path, token string, body any) wecomNameResponse {
	data, err := json.Marshal(body)
	if err != nil {
		return wecomNameResponse{}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://qyapi.weixin.qq.com"+path, bytes.NewReader(data))
	if err != nil {
		return wecomNameResponse{}
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return wecomNameResponse{}
	}
	defer resp.Body.Close()
	var result wecomNameResponse
	if resp.StatusCode != http.StatusOK ||
		json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result) != nil ||
		result.Code == nil || *result.Code != 0 {
		return wecomNameResponse{}
	}
	return result
}

func wecomCLINameResult(ctx context.Context, client *http.Client, path, token string, target any) bool {
	reply := wecomNameRequest(ctx, client, path, token, map[string]string{"payload": "{}"})
	var envelope struct {
		Result string `json:"result"`
	}
	return json.Unmarshal([]byte(reply.Results), &envelope) == nil &&
		json.Unmarshal([]byte(envelope.Result), target) == nil
}

func wecomBotName(identityContext string) string {
	var inBotBlock bool
	var name string
	for _, line := range strings.Split(identityContext, "\n") {
		switch line = strings.TrimSpace(line); {
		case line == "机器人身份：":
			if inBotBlock {
				return ""
			}
			inBotBlock = true
		case line == "授权真人用户身份：":
			return name
		case inBotBlock && strings.HasPrefix(line, "名字："):
			if name != "" {
				return ""
			}
			name = validWeComName(strings.TrimPrefix(line, "名字："))
		}
	}
	return name
}

func validWeComName(name string) string {
	name = strings.TrimSpace(name)
	if len([]rune(name)) > 255 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return ""
	}
	return name
}
