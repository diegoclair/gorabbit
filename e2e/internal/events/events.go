// Package events is the exchange the harness applications share: the publisher
// owns it and every consumer binds to it.
package events

import "github.com/diegoclair/gorabbit"

type Exchange struct{}

func (Exchange) Name() string { return "e2e.events" }

type msg = gorabbit.Msg[Exchange]

type routedMsg = gorabbit.RoutedMsg[Exchange]

// Batch and Seq identify a message at the application level, which is what lets
// the runner match what it asked for against what the broker and the cache hold.
type OrderPlaced struct {
	msg
	Batch string `json:"batch"`
	Seq   int    `json:"seq"`
}

type VendorEvent struct {
	routedMsg
	Vendor string `json:"vendor"`
	Batch  string `json:"batch"`
	Seq    int    `json:"seq"`
}

func (v VendorEvent) RouteBy() string { return v.Vendor }
