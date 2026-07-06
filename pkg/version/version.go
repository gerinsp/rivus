package version

import (
	"os"
	"runtime/debug"
	"strings"
)

var (
	Version   = "dev"
	Commit    = ""
	BuildDate = ""
)

type Info struct {
	Version   string `json:"version"`
	ImageTag  string `json:"image_tag"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
}

func Current() Info {
	version := firstNonEmpty(os.Getenv("RIVUS_VERSION"), Version, "dev")
	info := Info{
		Version:   version,
		ImageTag:  firstNonEmpty(os.Getenv("RIVUS_IMAGE_TAG"), version),
		Commit:    firstNonEmpty(os.Getenv("RIVUS_COMMIT"), Commit, buildCommit()),
		BuildDate: firstNonEmpty(os.Getenv("RIVUS_BUILD_DATE"), BuildDate),
	}
	return info
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func buildCommit() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return ""
}
