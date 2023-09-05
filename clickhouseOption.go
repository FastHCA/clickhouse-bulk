package main

type ClickhouseOption interface {
	apply(*Clickhouse)
	applyQueue(*Queue)
}

var _ ClickhouseOption = ClickhouseQueueOptionFunc(nil)

type ClickhouseQueueOptionFunc func(*Queue)

// apply implements ClickhouseOption.
func (ClickhouseQueueOptionFunc) apply(*Clickhouse) {}

// applyQueue implements ClickhouseOption.
func (fn ClickhouseQueueOptionFunc) applyQueue(q *Queue) {
	fn(q)
}

func WithNsqlookupdAddresses(addresses []string) ClickhouseOption {
	return ClickhouseQueueOptionFunc(func(q *Queue) {
		q.nsqlookupdAddresses = addresses
	})
}

func WithNsqdAddresses(addresses []string) ClickhouseOption {
	return ClickhouseQueueOptionFunc(func(q *Queue) {
		q.nsqdAddresses = addresses
	})
}

func WithTopic(topic string) ClickhouseOption {
	return ClickhouseQueueOptionFunc(func(q *Queue) {
		q.topic = topic
	})
}

func WithChannel(channel string) ClickhouseOption {
	return ClickhouseQueueOptionFunc(func(q *Queue) {
		q.channel = channel
	})
}
