# Beak 企业微信 Channel SDK

这个 Go module 把企业微信智能机器人长连接协议适配为 Beak Channel SDK 统一契约。

模块路径：

```text
github.com/GuanceCloud/beak-agent-wecom
```

平台标识：

```text
wecom
```

## 支持范围

- Bot ID + Secret 凭证接入。
- 企业微信智能机器人 WebSocket 长连接。
- 单聊和群聊文本入站。
- 图片、语音、文件、视频和图文混排入站的可读占位文本。
- 引用消息归一化为 `ReferencedMessage`。
- Beak 文本或 markdown 主动发送到原会话。
- UTF-8 安全的 20 KB markdown 分片。
- 入站去重、会话隔离、标准 runtime health。
- 凭证真实 WebSocket 鉴权。

当前不提供：

- 图片、文件、语音和视频的下载或上传。
- 模板卡片 builder。
- 流式更新同一条消息。
- reaction、typing 或 read acknowledgement。`Acknowledge` 会明确返回 `unsupported`。
- 外部联系人、客户群或企业微信客服场景。

## 接入前提

在企业微信管理端创建“智能机器人”，获得：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `bot_id` | 是 | 智能机器人的 Bot ID，也是稳定 account identity。 |
| `secret` | 是 | 智能机器人 Secret，必须加密保存。 |

不需要用户填写 callback URL、webhook URL、服务器 IP、域名或 WebSocket 地址。生产连接固定使用企业微信官方长连接入口。

同一个 Bot ID 同一时间只能保持一条有效长连接。新连接建立后，企业微信会向旧连接发送 `disconnected_event`。SDK 把它返回为 `Terminal=true`；Beak 必须停止该 runtime 并显示连接冲突，不能自动重连并与新连接互相抢占。

## Beak 运行时边界

这个 SDK 使用 `host_stream` 模式：

- Beak host 持有 WebSocket、重连、写锁、请求超时和 watchdog。
- SDK 持有企业微信鉴权帧、心跳帧、消息帧和回执帧语义。
- `Start` 只创建 Channel/Channel Link session 并初始化 account state，不创建连接。
- `ConnectStream` 返回 endpoint、初始鉴权帧和 `WaitForReady=true`。
- 收到鉴权成功回执后，`HandleStreamFrame` 返回 `Ready=true`。
- `Send` 通过 `Runtime.Stream.Request` 复用当前 account 的同一条 host-owned WebSocket。
- 企业微信回执的 `headers.req_id` 通过 `StreamFrameResult.ResponseTo` 交给 host 完成请求关联。

公共运行时必须包含：

```go
type Runtime struct {
	// 省略其他统一字段。
	Stream StreamTransport
}

type StreamTransport interface {
	Request(ctx context.Context, req StreamRequest) (*StreamResponse, error)
}
```

Beak adapter 不能解析企业微信 JSON，也不能按 `platform == "wecom"` 实现发送逻辑。它只负责在两个 module 的同形结构之间复制字段。

## 基本调用

```go
package main

import (
	"context"

	wecom "github.com/GuanceCloud/beak-agent-wecom"
	wecomsdk "github.com/GuanceCloud/beak-agent-wecom/sdk"
)

func main() {
	connector := wecom.NewConnector()

	result, err := connector.ValidateCredential(context.Background(), wecomsdk.CredentialValidationRequest{
		Credential: map[string]any{
			"bot_id": "your-bot-id",
			"secret": "your-secret",
		},
	})
	if err != nil || result == nil || !result.Valid {
		panic("invalid enterprise WeChat bot credential")
	}
}
```

`ValidateCredential` 会建立一条短生命周期 WebSocket，发送真实订阅鉴权帧并等待回执。它不是仅检查字段是否为空。由于企业微信限制单 Bot 单连接，不要把该方法作为高频健康检查；它应该只在创建或更新 account 时调用。

## 长连接握手

调用顺序必须是：

1. Beak 调 SDK `Start` 完成 account wiring。
2. Beak 调 `ConnectStream` 获取连接参数。
3. Beak 连接返回的 WebSocket URL。
4. Beak 按顺序写入 `InitialFrames`，其中第一帧是 `aibot_subscribe`。
5. Beak 把收到的帧交给 `HandleStreamFrame`。
6. SDK 校验鉴权回执并返回 `Ready=true`。
7. 只有此时 Beak 才把连接视为可发送，并启动业务请求。

鉴权失败会返回：

- `CloseReason`：可读错误。
- `Terminal=true`：不要重连无效凭证。
- `stream_connection_state=reconnect_failed`。
- 标准 error/reconnect error 时间字段。

## 入站归一化

| 企业微信字段 | Beak 字段 |
| --- | --- |
| `aibotid` | bot identity |
| `msgid` | `MessageID` 和 dedupe key |
| `chattype=single` | `ChatType=direct` |
| `chattype=group` | `ChatType=group` |
| 单聊 `from.userid` | `ChatID` |
| 群聊 `chatid` | `ChatID` |
| `from.userid` | `SenderID` |
| `quote` | `ReferencedMessage` |

企业微信智能机器人回调本身就是发给当前机器人的消息，因此单聊和群聊回调都会输出 `MentionedMe=true`。SDK 不要求 Beak 从正文重新识别 `@机器人`。

入站去重键为：

```text
wecom:<account_uuid>:<msgid>
```

会话 source key 仍使用统一 chat 维度：

```text
wecom:<account_uuid>:<chat_type>:<chat_id>
```

只有 `CreateMessage` 成功后才推进本地 dedupe state，避免处理失败时永久丢消息。

## 出站发送

Beak 始终调用统一 `Send`：

```go
result, err := connector.Send(ctx, runtime, wecomsdk.OutboundMessage{
	AccountUUID: accountUUID,
	ChatType:    wecomsdk.ChatTypeGroup,
	ChatID:      chatID,
	Text:        answer,
	Format:      "markdown",
})
```

SDK 使用 `aibot_send_msg` 主动发送 markdown。`ChatID` 的含义由入站归一化保证：单聊是用户 `userid`，群聊是平台 `chatid`。

企业微信单条 markdown 正文上限为 20480 UTF-8 bytes。超过上限时 SDK 在 rune 边界自动分片，不会切坏中文或 emoji。每一片都等待平台回执；任何一片失败时 `Send` 返回错误，不会假装整条消息成功。

## Runtime health

SDK 和 Beak 共同写统一 health key：

- `stream_connection_state`
- `stream_connected_at`
- `stream_disconnected_at`
- `stream_last_activity_at`
- `stream_last_ping_at`
- `stream_last_pong_at`
- `stream_last_event_at`
- `stream_last_error`
- `stream_last_error_at`
- `stream_reconnect_requested_at`
- `stream_reconnect_error`
- `stream_reconnect_error_at`

连接建立但鉴权未完成时状态是 `reconnecting`，不能提前显示 `connected`。鉴权成功后才写 `connected`。真实入站消息成功进入 Beak 后才更新 `stream_last_event_at`。

## 测试

```bash
go test ./...
```

发布前还必须运行工作区共享验收：

```bash
cd ../beak-channel-sdk-conformance
go test ./...

cd ../beak-channel-sdk-conformance-tests
go test ./...
```

共享场景覆盖真实 SDK 方法，不使用测试替身补齐 SDK 行为，包括真实 WebSocket 鉴权、鉴权就绪、心跳、直接/群聊入站、引用消息、去重、终止事件和 host-stream 发送回执。

## 安全要求

- `secret` 只能进入 encrypted credential，不能写入 account state、日志、错误信息或前端响应。
- 入站 `response_url` 是短生命周期授权值。当前 SDK 通过已鉴权的 host stream 出站，不能把该 URL 持久化到消息 metadata、account state 或日志。
- 日志可记录 account UUID、Bot ID、`req_id` 和错误码，但不能记录完整鉴权帧。
- `NativeRuntime.WebSocketURL` 只用于测试或受控部署覆盖，不属于 CredentialSchema，也不能暴露给普通用户。
- 不要用 `ValidateCredential` 做周期性 heartbeat；运行时健康由 host-owned stream 的标准 health 字段判断。
