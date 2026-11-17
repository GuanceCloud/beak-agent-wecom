package beakwecom

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	platform "github.com/GuanceCloud/beak-agent-wecom/internal/wecom"
	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

func TestDirectoryDisplayNameMatchesExactCallbackUser(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: displayNameTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodGet || req.URL.Host != "qyapi.weixin.qq.com" {
			t.Fatalf("unexpected directory request: %s %s", req.Method, req.URL.Host)
		}
		query := req.URL.Query()
		var body string
		switch req.URL.Path {
		case "/cgi-bin/gettoken":
			if query.Get("corpid") != "corp" || query.Get("corpsecret") != "directory-secret" {
				t.Fatal("missing configured application credential")
			}
			body = `{"errcode":0,"access_token":"token","expires_in":7200}`
		case "/cgi-bin/user/get":
			if query.Get("access_token") != "token" {
				t.Fatal("directory lookup did not use application token")
			}
			switch query.Get("userid") {
			case "member":
				body = `{"errcode":0,"userid":"member","name":"姓名"}`
			case "mismatch":
				body = `{"errcode":0,"userid":"someone-else","name":"另一个人"}`
			default:
				body = `{"errcode":60111}`
			}
		default:
			t.Fatalf("unexpected endpoint: %s", req.URL.Path)
		}
		return &http.Response{Body: io.NopCloser(strings.NewReader(body)), StatusCode: http.StatusOK}, nil
	})}
	resolver := NewDirectoryNameResolver(sdk.Runtime{HTTPClient: client}, sdk.ChannelAccount{
		Credential: map[string]any{"corp_id": "corp", "directory_app_secret": "directory-secret"},
	})
	ctx := context.Background()
	for _, tc := range []struct{ corp, userid, want string }{
		{"another-corp", "member", ""},
		{"corp", "member", "姓名"},
		{"corp", "member", "姓名"},
		{"", "mismatch", ""},
		{"corp", "no-access", ""},
	} {
		if got := resolver.DisplayName(ctx, tc.corp, tc.userid); got != tc.want {
			t.Errorf("directory name for %s = %q, want %q", tc.userid, got, tc.want)
		}
	}
	if calls != 4 {
		t.Errorf("made %d requests, want one token and three exact lookups", calls)
	}
}

func TestDirectoryDisplayNameIsOptional(t *testing.T) {
	resolver := NewDirectoryNameResolver(sdk.Runtime{}, sdk.ChannelAccount{Credential: map[string]any{"bot_id": "bot", "secret": "secret"}})
	if name := resolver.DisplayName(context.Background(), "", "member"); name != "" {
		t.Errorf("unconfigured directory returned %q", name)
	}
}

func TestPrivateCallbackUsesDirectoryName(t *testing.T) {
	for _, tc := range []struct{ directoryName, wantName string }{
		{`{"errcode":0,"userid":"user-1","name":"姓名"}`, "姓名"},
		{`{"errcode":48002}`, ""},
	} {
		account := testAccount()
		account.Credential[DirectoryCorpIDField] = "corp"
		account.Credential[DirectorySecretField] = "directory-secret"
		gateway := &testGateway{}
		runtime := testRuntime(account, gateway, &testAccountStore{})
		runtime.HTTPClient = &http.Client{Transport: displayNameTransport(func(req *http.Request) (*http.Response, error) {
			body := tc.directoryName
			if req.URL.Path == "/cgi-bin/gettoken" {
				body = `{"errcode":0,"access_token":"token","expires_in":7200}`
			}
			return &http.Response{Body: io.NopCloser(strings.NewReader(body)), StatusCode: http.StatusOK}, nil
		})}
		runtime.Native = NativeRuntime{DirectoryNames: NewDirectoryNameResolver(runtime, account)}
		data := mustFrameData(t, platform.Frame{
			Command: platform.CommandMessage,
			Headers: platform.FrameHeaders{RequestID: "private-callback"},
			Body: map[string]any{
				"msgid": "private-msg", "aibotid": "bot-1", "chattype": "single",
				"from":    map[string]any{"userid": "user-1", "corpid": "corp"},
				"msgtype": "text", "text": map[string]any{"content": "message"},
			},
		})
		result, err := (Connector{}).HandleStreamFrame(context.Background(), runtime, account,
			sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: data})
		if err != nil || result.EventResult == nil || result.EventResult.Inbound == nil {
			t.Fatalf("private callback: result=%+v err=%v", result, err)
		}
		inbound := result.EventResult.Inbound
		if inbound.ChatID != "user-1" || inbound.ChatIdentity.DisplayName != tc.wantName ||
			inbound.SenderDisplayName != tc.wantName || len(gateway.messages) != 1 {
			t.Fatalf("unexpected private message: %+v", inbound)
		}
	}
}
