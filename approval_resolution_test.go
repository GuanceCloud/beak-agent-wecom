package beakwecom

import (
	"testing"

	"github.com/GuanceCloud/beak-agent-wecom/sdk"
)

func TestToolApprovalShowsResolutionReason(t *testing.T) {
	value := map[string]any{}
	styleWecomToolApproval(value, sdk.InteractionCard{State: "expired", ToolApproval: &sdk.ToolApprovalDetails{ToolName: "exec", ResolutionText: "审批超过有效时间"}})
	title := value["main_title"].(map[string]string)
	if title["desc"] != "审批超过有效时间" {
		t.Fatalf("description = %q", title["desc"])
	}
}
