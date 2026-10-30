# Beak WeCom Channel SDK

This Go module adapts the WeCom AI Bot WebSocket protocol to the unified Beak Channel SDK contract.

Module:

```text
github.com/GuanceCloud/beak-agent-wecom
```

Platform key: `wecom`.

## Supported scope

- Bot ID and Secret credential login.
- Host-owned WeCom AI Bot WebSocket connection.
- Direct and group inbound messages.
- Readable placeholders for inbound image, voice, file, video, and mixed messages.
- Inbound media is downloaded (and decrypted when the platform provides an AES key) into a temporary local path before the host imports it.
- Quoted-message normalization through `ReferencedMessage`.
- Text or markdown delivery back to the original conversation.
- UTF-8-safe markdown splitting at the 20 KB platform limit.
- Inbound deduplication, chat isolation, and standard runtime health.
- Real WebSocket credential authentication.
- Outbound image, file, audio, and video attachments use the WeCom same-socket three-step media upload flow and native media messages.

Template cards, incremental message updates, reactions/typing/read acknowledgements, external contacts, customer groups, and WeCom customer-service conversations remain outside this connector's scope.

## Required credentials

Create a WeCom AI Bot in the WeCom administration console and obtain:

| Field | Required | Purpose |
| --- | --- | --- |
| `bot_id` | Yes | Stable AI Bot and account identity. |
| `secret` | Yes | AI Bot secret; Beak must store it encrypted. |

Users do not provide a callback URL, webhook URL, server IP, domain, or WebSocket URL. Production uses the official WeCom long-connection endpoint.

Only one active connection may exist for a Bot ID. A new connection causes WeCom to send `disconnected_event` to the old one. The SDK returns that event with `Terminal=true`; Beak must stop and fail that runtime instead of reconnecting two competing instances.

## Runtime ownership

The connector uses `host_stream` ownership:

- Beak owns the socket, reconnect lifecycle, write serialization, request timeouts, and watchdog.
- The SDK owns WeCom authentication, heartbeat, callback, and response-frame semantics.
- `Start` performs account wiring and state initialization only.
- `ConnectStream` returns the endpoint, authentication `InitialFrames`, and `WaitForReady=true`.
- `HandleStreamFrame` returns `Ready=true` only after a successful authentication response.
- `Send` reuses the current account connection through `Runtime.Stream.Request`.
- `StreamFrameResult.ResponseTo` maps a WeCom `headers.req_id` response back to the pending host request.

The common runtime surface is:

```go
type Runtime struct {
	// Other common fields omitted.
	Stream StreamTransport
}

type StreamTransport interface {
	Request(ctx context.Context, req StreamRequest) (*StreamResponse, error)
}
```

The Beak adapter must not parse WeCom JSON or add `platform == "wecom"` send branches. It only maps equivalent types between Go modules.

## Credential validation

```go
connector := wecom.NewConnector()
result, err := connector.ValidateCredential(ctx, wecomsdk.CredentialValidationRequest{
	Credential: map[string]any{
		"bot_id": "your-bot-id",
		"secret": "your-secret",
	},
})
```

Validation opens a short-lived WebSocket, sends the real subscription frame, and waits for the authentication response. It is not a non-empty-field check. Because WeCom allows one active connection per bot, use validation only when creating or updating an account, not as a periodic health probe.

## Connection sequence

1. Beak calls `Start` for account wiring.
2. Beak calls `ConnectStream`.
3. Beak dials the returned endpoint.
4. Beak writes `InitialFrames` in order.
5. Every received frame is passed to `HandleStreamFrame`.
6. A successful authentication response returns `Ready=true`.
7. Only then may Beak dispatch stream requests.

Authentication failures return a readable `CloseReason`, `Terminal=true`, `stream_connection_state=reconnect_failed`, and the standard error timestamps. Retrying an invalid secret indefinitely is prohibited.

## Inbound mapping

| WeCom field | Beak field |
| --- | --- |
| `aibotid` | Bot identity |
| `msgid` | `MessageID` and dedupe identity |
| `chattype=single` | `ChatType=direct` |
| `chattype=group` | `ChatType=group` |
| Direct `from.userid` | `ChatID` |
| Group `chatid` | `ChatID` |
| `from.userid` | `SenderID` |
| `quote` | `ReferencedMessage` |

AI Bot callbacks are addressed to the current bot, so direct and group callbacks are normalized with `MentionedMe=true`. Beak does not reparse the message text for mentions.

Deduplication uses:

```text
wecom:<account_uuid>:<msgid>
```

Session identity remains chat scoped:

```text
wecom:<account_uuid>:<chat_type>:<chat_id>
```

The SDK records a message as seen only after `CreateMessage` succeeds.

## Outbound delivery

`Send` uses the WeCom `aibot_send_msg` command with markdown content. For direct chats, `ChatID` is the user's `userid`; for group chats, it is the platform `chatid`.

WeCom limits one markdown body to 20,480 UTF-8 bytes. The SDK splits larger output on rune boundaries, waits for the platform response to every chunk, and returns an error as soon as any chunk fails.

## Runtime health

The connector uses the common health keys, including connection state, connected/disconnected timestamps, last activity, ping, pong, event, error, and reconnect fields. A socket that has connected but not authenticated remains `reconnecting`; it becomes `connected` only after the authentication response. `stream_last_event_at` changes only after an inbound message reaches Beak.

## Verification

```bash
go test ./...
```

Also run the workspace `beak-channel-sdk-conformance` and `beak-channel-sdk-conformance-tests` modules. The shared scenarios cover the real SDK methods for authentication, readiness, heartbeat, direct/group inbound, quotes, dedupe, terminal disconnect, and correlated host-stream responses.

## Security

- Keep `secret` only in encrypted credentials; never copy it into account state or logs.
- Treat inbound `response_url` as a short-lived authorization value. This SDK uses the authenticated host stream for outbound delivery and deliberately does not persist or expose that URL in normalized message metadata.
- Logs may include account UUID, Bot ID, request ID, and platform error code, but never the full authentication frame.
- `NativeRuntime.WebSocketURL` is a test or controlled-deployment override. It is not part of `CredentialSchema` and must not be shown to ordinary users.
- Do not call `ValidateCredential` as a heartbeat. Runtime health comes from the active host-owned stream.

## Approval details delivery

Native WeCom clients may collapse the operation preview. The host sends complete redacted details before the actionable card, using the same approval request ID. The card now labels its quote area as a preview and displays that request ID. The host owns durable delivery progress and must not send an actionable card before details are acknowledged. Terminal callbacks continue updating the original card.
