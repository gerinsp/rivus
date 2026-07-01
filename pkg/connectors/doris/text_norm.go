package doris

import "unicode/utf8"

var asciiRepl = map[rune]rune{
	'–': '-', '—': '-', '−': '-',
	'“': '\'', '”': '\'', '‘': '\'', '’': '\'',
	'\u00A0': ' ', // nbsp
}

func normalizeToASCII(s string) string {
	if s == "" {
		return s
	}
	// 1) replace beberapa karakter umum
	var b []rune
	b = make([]rune, 0, len(s))
	for _, r := range []rune(s) {
		if rr, ok := asciiRepl[r]; ok {
			r = rr
		}
		// 2) buang non-ascii
		if r >= 0 && r < 128 {
			b = append(b, r)
		}
	}
	return string(b)
}

// truncate by "characters" (rune count). Setelah normalizeToASCII, rune count == byte count.
func truncateChars(s string, max int) string {
	if max <= 0 || s == "" {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// kalau kamu suatu saat mau truncate by BYTES (lebih ketat), pakai ini:
func truncateUTF8Bytes(s string, max int) string {
	if max <= 0 || s == "" {
		return s
	}
	b := []byte(s)
	if len(b) <= max {
		return s
	}
	b = b[:max]
	for len(b) > 0 && !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b)
}
