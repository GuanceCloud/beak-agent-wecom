package beakwecom

import (
	"strings"
	"testing"

	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

func TestApprovalCardMatchesSeparateDetails(t *testing.T) {
	for _, language := range []string{"zh", "en"} {
		card := sdk.InteractionCard{RequestID: "approval_details", Revision: 1, Kind: "tool_approval", State: "pending", Language: language, Title: "Approval", ToolApproval: &sdk.ToolApprovalDetails{ToolName: "exec", ArgumentsPreview: strings.Repeat("x", 128)}, Actions: []sdk.InteractionButton{{ID: "approve", Label: "Approve"}, {ID: "reject", Label: "Reject"}}}
		payload, err := wecomInteractionCard(card)
		if err != nil {
			t.Fatal(err)
		}
		label := "Request: "
		preview := "Operation (preview)"
		if language == "zh" {
			label = "审批编号："
			preview = "执行内容（摘要）"
		}
		if payload["sub_title_text"] != label+card.RequestID {
			t.Fatalf("missing request reference: %v", payload["sub_title_text"])
		}
		if payload["quote_area"].(map[string]any)["title"] != preview {
			t.Fatal("client-collapsed arguments must be labeled as a preview")
		}
	}
}
