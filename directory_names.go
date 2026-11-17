package beakwecom

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

const (
	DirectoryCorpIDField = "corp_id"
	DirectorySecretField = "directory_app_secret"
)

type directoryName struct {
	name      string
	expiresAt time.Time
}

// DirectoryNameResolver is scoped to one configured WeCom account.
type DirectoryNameResolver struct {
	corpID, secret string
	client         http.Client
	gate           chan struct{}
	token          string
	tokenExpiry    time.Time
	names          map[string]directoryName
}

// NewDirectoryNameResolver uses an optional internal application from the bot's
// enterprise. Without both credentials there is no directory lookup.
func NewDirectoryNameResolver(runtime sdk.Runtime, account sdk.ChannelAccount) *DirectoryNameResolver {
	corpID := strings.TrimSpace(stringValue(account.Credential[DirectoryCorpIDField]))
	secret := strings.TrimSpace(stringValue(account.Credential[DirectorySecretField]))
	if corpID == "" || secret == "" {
		return nil
	}
	client := http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	if runtime.HTTPClient != nil {
		client.Transport = runtime.HTTPClient.Transport
	}
	return &DirectoryNameResolver{
		corpID: corpID, secret: secret, client: client,
		gate: make(chan struct{}, 1), names: make(map[string]directoryName),
	}
}

// DisplayName queries one callback user in the configured enterprise. A denied,
// ambiguous, or slow lookup leaves the original message path available.
func (r *DirectoryNameResolver) DisplayName(ctx context.Context, callbackCorpID, userID string) string {
	userID = strings.TrimSpace(userID)
	callbackCorpID = strings.TrimSpace(callbackCorpID)
	if r == nil || userID == "" || (callbackCorpID != "" && callbackCorpID != r.corpID) {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	case <-ctx.Done():
		return ""
	}
	now := time.Now()
	if value, ok := r.names[userID]; ok && now.Before(value.expiresAt) {
		return value.name
	}
	if now.After(r.tokenExpiry) {
		reply := r.request(ctx, "/cgi-bin/gettoken", url.Values{"corpid": {r.corpID}, "corpsecret": {r.secret}})
		if reply.Token == "" || reply.ExpiresIn <= 0 {
			return ""
		}
		r.token = reply.Token
		validity := time.Duration(reply.ExpiresIn) * time.Second
		if validity > time.Minute {
			validity -= time.Minute
		}
		r.tokenExpiry = now.Add(validity)
	}

	reply := r.request(ctx, "/cgi-bin/user/get", url.Values{"access_token": {r.token}, "userid": {userID}})
	if reply.Code != nil && (*reply.Code == 40014 || *reply.Code == 42001) {
		r.tokenExpiry = time.Time{}
	}
	if reply.UserID != userID {
		return ""
	}
	name := validWeComName(reply.Name)
	if name != "" {
		if len(r.names) >= 256 {
			clear(r.names)
		}
		r.names[userID] = directoryName{name: name, expiresAt: now.Add(10 * time.Minute)}
	}
	return name
}

type directoryReply struct {
	Code      *int   `json:"errcode"`
	Token     string `json:"access_token"`
	ExpiresIn int    `json:"expires_in"`
	UserID    string `json:"userid"`
	Name      string `json:"name"`
}

func (r *DirectoryNameResolver) request(ctx context.Context, path string, query url.Values) directoryReply {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://qyapi.weixin.qq.com"+path+"?"+query.Encode(), nil)
	if err != nil {
		return directoryReply{}
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return directoryReply{}
	}
	defer resp.Body.Close()
	var reply directoryReply
	if resp.StatusCode != http.StatusOK ||
		json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&reply) != nil ||
		reply.Code == nil || *reply.Code != 0 {
		return directoryReply{Code: reply.Code}
	}
	return reply
}
