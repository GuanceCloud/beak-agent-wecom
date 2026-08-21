package sdk

import (
	"fmt"
	"net/url"
	"strings"
)

func ValidateMediaAttachment(attachment MediaAttachment) error {
	switch strings.TrimSpace(attachment.Kind) {
	case MediaKindImage, MediaKindFile, MediaKindAudio, MediaKindVideo, MediaKindSticker:
	default:
		return fmt.Errorf("unsupported attachment kind %q", attachment.Kind)
	}
	switch strings.TrimSpace(attachment.Source) {
	case "":
	case MediaSourcePath:
		if strings.TrimSpace(attachment.Path) == "" {
			return fmt.Errorf("attachment path is required")
		}
	case MediaSourceURL:
		parsed, err := url.Parse(strings.TrimSpace(attachment.URL))
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("attachment URL must be absolute HTTP or HTTPS")
		}
	case MediaSourcePlatformResource:
		if strings.TrimSpace(attachment.PlatformResourceID) == "" {
			return fmt.Errorf("platform resource ID is required")
		}
	default:
		return fmt.Errorf("unsupported attachment source %q", attachment.Source)
	}
	if attachment.SizeBytes < 0 || attachment.DurationMillis < 0 {
		return fmt.Errorf("attachment size and duration must not be negative")
	}
	return nil
}

func ValidateMediaAttachments(attachments []MediaAttachment) error {
	for index, attachment := range attachments {
		if err := ValidateMediaAttachment(attachment); err != nil {
			return fmt.Errorf("attachment %d: %w", index, err)
		}
	}
	return nil
}
