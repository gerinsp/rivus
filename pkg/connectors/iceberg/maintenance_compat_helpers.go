package iceberg

func cloneStringAnyMap(in map[string]any) map[string]any {
	return copyAnyMap(in)
}

// copyAnyMap and stringAnyMap used to live beside the legacy automatic
// monitor. They are general config helpers, so keep them after retiring that
// scheduler.
func copyAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch nested := value.(type) {
		case map[string]any:
			out[key] = copyAnyMap(nested)
		case []any:
			items := make([]any, len(nested))
			for i, item := range nested {
				if child, ok := item.(map[string]any); ok {
					items[i] = copyAnyMap(child)
				} else {
					items[i] = item
				}
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}

func stringAnyMap(value any) map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return typed
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			name, ok := key.(string)
			if !ok {
				continue
			}
			out[name] = item
		}
		return out
	default:
		return nil
	}
}
