package util

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var mysqlDatePrefixPattern = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})(.*)$`)

// NormalizeMySQLPartialDate converts MySQL's zero date components to valid
// values while preserving every known component and any time suffix.
func NormalizeMySQLPartialDate(raw string) (string, bool) {
	parts := mysqlDatePrefixPattern.FindStringSubmatch(strings.TrimSpace(raw))
	if len(parts) != 5 {
		return raw, false
	}
	year, _ := strconv.Atoi(parts[1])
	month, _ := strconv.Atoi(parts[2])
	day, _ := strconv.Atoi(parts[3])
	if year != 0 && month != 0 && day != 0 {
		return raw, false
	}
	if year == 0 {
		year = 1970
	}
	if month == 0 {
		month = 1
	}
	if day == 0 {
		day = 1
	}
	return fmt.Sprintf("%04d-%02d-%02d%s", year, month, day, parts[4]), true
}
