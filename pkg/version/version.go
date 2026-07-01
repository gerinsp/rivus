package version

import (
	"os"
	"strings"
)

var (
	Version   = "dev"
	Commit    = ""
	BuildDate = ""
)

type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
}

func Current() Info {
	info := Info{
		Version:   firstNonEmpty(os.Getenv("RIVUS_VERSION"), Version, "dev"),
		Commit:    firstNonEmpty(os.Getenv("RIVUS_COMMIT"), Commit),
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
