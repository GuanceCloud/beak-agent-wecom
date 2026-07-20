package wecom

import "testing"

func TestMessageAndQuoteText(t *testing.T) {
	message := Message{
		MessageType: MessageTypeMixed,
		Mixed: &MixedContent{Items: []MixedItem{
			{MessageType: MessageTypeText, Text: &TextContent{Content: "first"}},
			{MessageType: MessageTypeImage, Image: &MediaContent{URL: "https://example.test/image"}},
			{MessageType: MessageTypeText, Text: &TextContent{Content: "second"}},
		}},
	}
	if got := MessageText(message); got != "first\n[图片]\nsecond" {
		t.Fatalf("text=%q", got)
	}
	if got := QuoteText(&QuoteContent{MessageType: MessageTypeFile, File: &MediaContent{URL: "https://example.test/file"}}); got != "[文件]" {
		t.Fatalf("quote=%q", got)
	}
}

func TestFrameRequiresRequestID(t *testing.T) {
	if _, err := MarshalFrame(Frame{}); err == nil {
		t.Fatal("expected missing req_id error")
	}
	if _, err := ParseFrame([]byte(`{"headers":{}}`)); err == nil {
		t.Fatal("expected missing req_id error")
	}
}
