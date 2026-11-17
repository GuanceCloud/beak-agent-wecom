package beakwecom

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

type displayNameTransport func(*http.Request) (*http.Response, error)

func (f displayNameTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestLookupDisplayNames(t *testing.T) {
	account := sdk.ChannelAccount{Credential: map[string]any{"bot_id": "qa-bot", "secret": "test"}}
	runtime := sdk.Runtime{HTTPClient: &http.Client{Transport: displayNameTransport(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Host != "qyapi.weixin.qq.com" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
		}
		body := ""
		switch req.URL.Path {
		case "/cgi-bin/aibot/cli/get_cli_config":
			var auth struct {
				BotID      string `json:"bot_id"`
				Time       int64  `json:"time"`
				Nonce      string `json:"nonce"`
				Signature  string `json:"signature"`
				BindSource int    `json:"bind_source"`
			}
			if err := json.NewDecoder(req.Body).Decode(&auth); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("testqa-bot%d%s", auth.Time, auth.Nonce))))
			if auth.BotID != "qa-bot" || auth.Signature != want || !strings.HasPrefix(auth.Nonce, "cli_") || auth.BindSource != 1 {
				t.Fatal("invalid credential bootstrap")
			}
			body = `{"errcode":0,"token":"qa-token"}`
		case "/cli/identity/whoami":
			body = `{"errcode":0,"results_json":"{\"result\":\"{\\\"extra_identity_context\\\":\\\"机器人身份：\\\\n名字：QA Bot\\\\nID：qa-bot\\\\n授权真人用户身份：\\\\n名字：Owner\\\\nID：owner\\\"}\"}"}`
		case "/cli/message/aibot/sessions/list":
			body = `{"errcode":0,"results_json":"{\"result\":\"{\\\"sessions\\\":[{\\\"chat_id\\\":\\\"shared\\\",\\\"chat_type\\\":\\\"group\\\",\\\"chat_name\\\":\\\"群聊\\\"},{\\\"chat_id\\\":\\\"shared\\\",\\\"chat_type\\\":\\\"single\\\",\\\"chat_name\\\":\\\"用户\\\"}]}\"}"}`
		default:
			t.Fatalf("unexpected endpoint: %s", req.URL.Path)
		}
		if !strings.HasPrefix(req.URL.Path, "/cgi-bin/") && req.Header.Get("Authorization") != "Bearer qa-token" {
			t.Fatal("platform request missing token")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	names := (Connector{}).LookupDisplayNames(context.Background(), runtime, account)
	for _, check := range []struct{ got, want string }{
		{names.BotName, "QA Bot"},
		{names.ConversationName(sdk.ChatTypeGroup, "shared"), "群聊"},
		{names.ConversationName(sdk.ChatTypeDirect, "shared"), "用户"},
		{names.ConversationName(sdk.ChatTypeDirect, "unknown"), ""},
		{names.ConversationName(sdk.ChatTypeDirect, "Shared"), ""},
	} {
		if check.got != check.want {
			t.Errorf("name = %q, want %q", check.got, check.want)
		}
	}
}

func TestWeComBotNameSkipsAuthorizedUserName(t *testing.T) {
	for _, context := range []string{
		"授权真人用户身份：\n名字：Owner",
		"机器人身份：\nID：bot\n授权真人用户身份：\n名字：Owner",
		"机器人身份：\n名字：one\n名字：two",
	} {
		if name := wecomBotName(context); name != "" {
			t.Errorf("ambiguous bot name = %q", name)
		}
	}
}
