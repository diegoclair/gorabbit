package gorabbit

import (
	"encoding/json/v2"
	"os/exec"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type ordersExchange struct{}

func (ordersExchange) Name() string { return "orders" }

type orders = Msg[ordersExchange]

type orderCreated struct {
	orders
	OrderID string `json:"order_id"`
}

type auditedOrderCreated struct {
	orderCreated
	Auditor string `json:"auditor"`
}

type routedOrders = RoutedMsg[ordersExchange]

type orderShipped struct {
	routedOrders
	Carrier string `json:"carrier"`
}

func (o orderShipped) RouteBy() string { return o.Carrier }

func TestExchangeOfResolvesTheMarker(t *testing.T) {
	require.Equal(t, "orders", ExchangeOf(orderCreated{}))
	require.Equal(t, "orders", ExchangeOf(&orderCreated{}))

	// Promotion crosses more than one level of embedding.
	require.Equal(t, "orders", ExchangeOf(auditedOrderCreated{}))
}

func TestRoutedMarkerResolvesTheExchangeAndTheRoute(t *testing.T) {
	require.Equal(t, "orders", ExchangeOf(orderShipped{}))

	require.True(t, IsRouted(orderShipped{}))
	require.False(t, IsRouted(orderCreated{}))

	route, ok := RouteOf(orderShipped{Carrier: "correios"})
	require.True(t, ok)
	require.Equal(t, "correios", route)

	_, ok = RouteOf(orderCreated{})
	require.False(t, ok, "a plain message has no route to give")
}

// A type that embeds the routed marker and never implements RouteBy is routed
// and broken, not plain: a driver must report it instead of routing it as if it
// had no route.
func TestRoutedMarkerWithoutRouteByIsStillRouted(t *testing.T) {
	type eventRecorded struct {
		routedOrders
	}

	require.True(t, IsRouted(eventRecorded{}))

	_, ok := RouteOf(eventRecorded{})
	require.False(t, ok)
}

func TestMarkerStaysOutOfTheJSONPayload(t *testing.T) {
	body, err := json.Marshal(orderCreated{OrderID: "123"})
	require.NoError(t, err)
	require.JSONEq(t, `{"order_id":"123"}`, string(body))

	body, err = json.Marshal(struct {
		Msg[ordersExchange]
		OrderID string `json:"order_id"`
	}{OrderID: "123"})
	require.NoError(t, err)
	require.JSONEq(t, `{"order_id":"123"}`, string(body), "the exported embed must not show up either")

	var decoded orderCreated
	require.NoError(t, json.Unmarshal([]byte(`{"order_id":"123"}`), &decoded))
	require.Equal(t, "123", decoded.OrderID)
}

// The marker is sealed: it exposes nothing an application could call, and no
// MarshalJSON that would hijack the serialization of the message embedding it.
func TestMarkerIsSealed(t *testing.T) {
	markerType := reflect.TypeOf(Msg[ordersExchange]{})

	require.Zero(t, markerType.NumField())
	require.Zero(t, markerType.NumMethod(), "no exported methods")
	require.Zero(t, reflect.PointerTo(markerType).NumMethod())

	_, hasMarshaler := any(Msg[ordersExchange]{}).(json.Marshaler)
	require.False(t, hasMarshaler)

	routedType := reflect.TypeOf(RoutedMsg[ordersExchange]{})

	require.Zero(t, routedType.NumField())
	require.Zero(t, routedType.NumMethod(), "no exported methods")
	require.Zero(t, reflect.PointerTo(routedType).NumMethod())
}

// The whole point of the marker: a message without one is rejected by the
// compiler, not by the broker at runtime.
func TestMessageWithoutAMarkerDoesNotCompile(t *testing.T) {
	require.Contains(t, buildTestdata(t, "nomarker"), "missing method exchangeName")
}

// The route is the message's own business, so the type has to say what it
// routes by before anything can subscribe to one of its routes.
func TestRoutedMessageWithoutRouteByDoesNotCompile(t *testing.T) {
	require.Contains(t, buildTestdata(t, "missingrouteby"), "does not satisfy gorabbit.RoutedMessage")
}

// The whole point of the two markers: only a message that carries a route can
// be sliced by one.
func TestSubscribingARouteOfAPlainMessageDoesNotCompile(t *testing.T) {
	require.Contains(t, buildTestdata(t, "plainroute"), "does not satisfy gorabbit.RoutedMessage")
}

// The marker, not RouteBy, is what a per-route subscription asks for: a plain
// message declaring one would bind on the route and publish on the bare type.
func TestPlainMessageDeclaringRouteByDoesNotCompile(t *testing.T) {
	require.Contains(t, buildTestdata(t, "fakerouted"), "missing method routedMsg")
}

// One message, one exchange, one routing shape.
func TestTwoMarkersDoNotCompile(t *testing.T) {
	require.Contains(t, buildTestdata(t, "twomarkers"), "ambiguous selector")
}

// Ownership is compile-time too: a client publishes its own exchange only.
func TestPublishingAnotherExchangeDoesNotCompile(t *testing.T) {
	require.Contains(t, buildTestdata(t, "crossexchange"), "wrong type for method ownedBy")
}

func buildTestdata(t *testing.T, pkg string) string {
	t.Helper()

	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain is not available")
	}

	out, err := exec.Command("go", "build", "./testdata/"+pkg).CombinedOutput()
	require.Error(t, err, "package %s must not compile, output: %s", pkg, out)

	return string(out)
}
