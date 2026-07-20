package sdk

const (
	RuntimeHealthKeyStreamConnectionState      = "stream_connection_state"
	RuntimeHealthKeyStreamConnectedAt          = "stream_connected_at"
	RuntimeHealthKeyStreamDisconnectedAt       = "stream_disconnected_at"
	RuntimeHealthKeyStreamLastActivityAt       = "stream_last_activity_at"
	RuntimeHealthKeyStreamLastPingAt           = "stream_last_ping_at"
	RuntimeHealthKeyStreamLastPongAt           = "stream_last_pong_at"
	RuntimeHealthKeyStreamLastEventAt          = "stream_last_event_at"
	RuntimeHealthKeyStreamLastError            = "stream_last_error"
	RuntimeHealthKeyStreamLastErrorAt          = "stream_last_error_at"
	RuntimeHealthKeyStreamReconnectRequestedAt = "stream_reconnect_requested_at"
	RuntimeHealthKeyStreamReconnectError       = "stream_reconnect_error"
	RuntimeHealthKeyStreamReconnectErrorAt     = "stream_reconnect_error_at"
	RuntimeHealthKeyStreamSessionExpired       = "stream_session_expired"
)

const (
	RuntimeHealthStateConnected       = "connected"
	RuntimeHealthStateReconnecting    = "reconnecting"
	RuntimeHealthStateReconnectFailed = "reconnect_failed"
	RuntimeHealthStateStopped         = "stopped"
	RuntimeHealthStateExpired         = "expired"
)
