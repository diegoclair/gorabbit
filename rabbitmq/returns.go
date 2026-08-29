package rabbitmq

import (
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// returnTracker exists because the broker returns an unroutable message before
// confirming it, so a confirm alone cannot be read as a delivery.
type returnTracker struct {
	queries  chan returnQuery
	finished chan struct{}
}

type returnQuery struct {
	msgID  string
	answer chan bool
}

// The amqp091 reader goroutine hands a return over on an unbuffered channel, so
// by the time the confirm of that message can be decoded the tracker has it. A
// query travelling through the same loop therefore cannot overtake it.
func newReturnTracker(ch *amqp091.Channel) *returnTracker {
	t := &returnTracker{
		queries:  make(chan returnQuery),
		finished: make(chan struct{}),
	}

	go t.track(ch.NotifyReturn(make(chan amqp091.Return)))

	return t
}

func (t *returnTracker) track(returns <-chan amqp091.Return) {
	defer close(t.finished)

	returned := make(map[string]struct{})

	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				return
			}
			returned[ret.MessageId] = struct{}{}
		case query := <-t.queries:
			_, was := returned[query.msgID]
			delete(returned, query.msgID)
			query.answer <- was
		}
	}
}

// wasReturned drops what it reports, so the tracker only ever holds the returns
// of publishes still waiting for their confirm.
func (t *returnTracker) wasReturned(msgID string) bool {
	if msgID == "" {
		return false
	}

	query := returnQuery{msgID: msgID, answer: make(chan bool, 1)}

	select {
	case t.queries <- query:
	case <-t.finished:
		return false
	}

	select {
	case was := <-query.answer:
		return was
	case <-t.finished:
		return false
	}
}
