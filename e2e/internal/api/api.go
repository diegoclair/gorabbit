// Package api is the HTTP contract between the harness applications and the
// runner, kept in one place so a driving call and its reply cannot drift.
package api

type Health struct {
	App           string   `json:"app"`
	Queue         string   `json:"queue,omitempty"`
	Connected     bool     `json:"connected"`
	Subscriptions []string `json:"subscriptions,omitempty"`
}

type PublishRequest struct {
	Kind    string `json:"kind"`
	Route   string `json:"route"`
	Batch   string `json:"batch"`
	Count   int    `json:"count"`
	DelayMS int    `json:"delay_ms"`
	Async   bool   `json:"async"`
}

type BatchStats struct {
	Requested int      `json:"requested"`
	Attempted int      `json:"attempted"`
	OK        int      `json:"ok"`
	Failed    int      `json:"failed"`
	Errors    []string `json:"errors,omitempty"`
}

type TwinResult struct {
	Connected bool   `json:"connected"`
	Error     string `json:"error"`
}

type Item struct {
	Kind       string `json:"kind"`
	Vendor     string `json:"vendor"`
	Batch      string `json:"batch"`
	Seq        int    `json:"seq"`
	Deliveries int    `json:"deliveries"`
}

type Received struct {
	App        string `json:"app"`
	Queue      string `json:"queue"`
	Total      int    `json:"total"`
	Unique     int    `json:"unique"`
	Duplicates int    `json:"duplicates"`
	Items      []Item `json:"items"`
}
