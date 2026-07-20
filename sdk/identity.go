package sdk

import "strings"

func ChatSourceID(platform, accountUUID, chatType, chatID string) string {
	return strings.TrimSpace(platform) + ":" + strings.TrimSpace(accountUUID) + ":" + strings.TrimSpace(chatType) + ":" + strings.TrimSpace(chatID)
}

func IMPersonParticipantID(platform, chatType, chatID, senderID string) string {
	return "im:" + strings.TrimSpace(platform) + ":" + strings.TrimSpace(chatType) + ":" + strings.TrimSpace(chatID) + ":user:" + strings.TrimSpace(senderID)
}

func BridgeParticipantID(platform string) string {
	return "bridge:" + strings.TrimSpace(platform)
}
