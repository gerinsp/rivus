package iceberg

func cloneStringAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch nested := value.(type) {
		case map[string]any:
			out[key] = cloneStringAnyMap(nested)
		default:
			out[key] = value
		}
	}
	return out
}
