package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type metaKeyPayload struct {
	Version int    `json:"v"`
	JobID   string `json:"job_id"`
	Mode    string `json:"mode"`

	Source struct {
		Type   string      `json:"type"`
		Config interface{} `json:"config"`
	} `json:"source"`

	Sink struct {
		Type   string      `json:"type"`
		Config interface{} `json:"config"`
	} `json:"sink"`
}

func buildMetaKey(jobID, mode, srcType string, srcCfg any, sinkType string, sinkCfg any) string {
	var p metaKeyPayload
	p.Version = 1
	p.JobID = jobID
	p.Mode = mode
	p.Source.Type = srcType
	p.Source.Config = srcCfg
	p.Sink.Type = sinkType
	p.Sink.Config = stableSinkConfigForMetaKey(sinkCfg)

	b, _ := json.Marshal(p) // deterministic for maps
	sum := sha256.Sum256(b)

	return "rivus/v1/" + hex.EncodeToString(sum[:])
}

func stableSinkConfigForMetaKey(cfg any) any {
	m, ok := cfg.(map[string]any)
	if !ok {
		return cfg
	}
	return copyMapWithoutKeys(m, map[string]struct{}{
		"cdc_delete_executor": {},
	})
}

func copyMapWithoutKeys(in map[string]any, skip map[string]struct{}) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		if _, ignored := skip[key]; ignored {
			continue
		}
		out[key] = copyMetaKeyValue(value, skip)
	}
	return out
}

func copyMetaKeyValue(value any, skip map[string]struct{}) any {
	switch v := value.(type) {
	case map[string]any:
		return copyMapWithoutKeys(v, skip)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = copyMetaKeyValue(item, skip)
		}
		return out
	default:
		return value
	}
}
