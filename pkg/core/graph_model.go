package core

type GraphNodeType string

const (
	NodeSource    GraphNodeType = "source"
	NodeBuffer    GraphNodeType = "buffer"
	NodeTransform GraphNodeType = "transform"
	NodeSink      GraphNodeType = "sink"
)

type GraphMetric struct {
	Label string `json:"label"`
	Value string `json:"value"`
	Tone  string `json:"tone,omitempty"`
}

type GraphNode struct {
	ID       string        `json:"id"`
	Type     GraphNodeType `json:"type"`
	Label    string        `json:"label"`
	Subtitle string        `json:"subtitle,omitempty"`
	Detail   string        `json:"detail,omitempty"`
	State    string        `json:"state,omitempty"`
	Status   JobStatus     `json:"status"`
	Metrics  []GraphMetric `json:"metrics,omitempty"`
}

type GraphEdge struct {
	From    string        `json:"from"`
	To      string        `json:"to"`
	Label   string        `json:"label,omitempty"`
	Detail  string        `json:"detail,omitempty"`
	State   string        `json:"state,omitempty"`
	Metrics []GraphMetric `json:"metrics,omitempty"`
}

type JobGraph struct {
	JobID    string       `json:"job_id"`
	Status   JobStatus    `json:"status"`
	Progress *JobProgress `json:"progress,omitempty"`
	Nodes    []GraphNode  `json:"nodes"`
	Edges    []GraphEdge  `json:"edges"`
}
