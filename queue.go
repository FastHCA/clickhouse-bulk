package main

import (
	"bytes"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/nikepan/go-datastructures/queue"
	"github.com/nsqio/go-nsq"
	"github.com/tinylib/msgp/msgp"
)

const (
	MaxInFlight = 32
)

var (
	ReservedLocalQueueSize = MaxInFlight*runtime.NumCPU()*2 + MaxInFlight
)

type Queue struct {
	local               *queue.Queue
	consumer            *nsq.Consumer
	producers           []*nsq.Producer
	nsqConfig           *nsq.Config
	nsqlookupdAddresses []string
	nsqdAddresses       []string

	topic   string
	channel string

	currentProducer int32
}

func NewQueue(hint int64) *Queue {
	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxInFlight = 0
	nsqConfig.MaxAttempts = 32

	instance := &Queue{
		local:     queue.New(hint),
		nsqConfig: nsqConfig,
	}

	{
		consumer, err := nsq.NewConsumer(instance.topic, instance.channel, instance.nsqConfig)
		if err != nil {
			panic(err)
		}
		consumer.AddConcurrentHandlers(nsq.HandlerFunc(instance.handleNsqMessage), runtime.NumCPU()*2)
		err = consumer.ConnectToNSQLookupds(instance.nsqlookupdAddresses)
		if err != nil {
			panic(err)
		}
		instance.consumer = consumer
	}

	for _, addr := range instance.nsqdAddresses {
		procuder, err := nsq.NewProducer(addr, instance.nsqConfig)
		if err != nil {
			panic(err)
		}
		instance.producers = append(instance.producers, procuder)
	}

	return instance
}

// Empty implements Queue.
func (q *Queue) Empty() bool {
	return q.local.Empty()
}

// Len implements Queue.
func (q *Queue) Len() int64 {
	return q.local.Len()
}

// Poll implements Queue.
func (q *Queue) Poll(count int64, timeout time.Duration) ([]interface{}, error) {
	data, err := q.local.Poll(count, timeout)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		q.consumer.ChangeMaxInFlight(MaxInFlight)
	}
	return data, nil
}

// Put implements Queue.
func (q *Queue) Put(items ...*ClickhouseRequest) error {
	for _, r := range items {
		if r == nil {
			continue
		}

		body, err := r.MarshalMsg(nil)
		if err != nil {
			return err
		}
		err = q.sendToNsq(q.topic, body)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Stop() {
	q.consumer.Stop()
}

func (q *Queue) sendToNsq(topic string, body []byte) error {
	var (
		size     = len(q.producers)
		attempts = size

		err  error
		next int32
	)

	for attempt := 0; attempt < attempts; attempt++ {
		next = q.nextProducer()

		producer := q.producers[next]
		err = producer.Publish(topic, body)
		if err != nil {
			continue
		}
		break
	}
	if err != nil {
		return err
	}
	return err
}

func (q *Queue) nextProducer() int32 {
	var (
		ubound int32 = int32(len(q.producers)) - 1
	)
	if ubound == 0 {
		return 0
	}

	// set p.current to 0, if it reach ubound
	if atomic.CompareAndSwapInt32(&q.currentProducer, ubound, 0) {
		return 0
	}
	next := atomic.AddInt32(&q.currentProducer, 1)

	return next
}

func (q *Queue) handleNsqMessage(message *nsq.Message) error {
	if q.local.Len() < int64(ReservedLocalQueueSize) {
		q.consumer.ChangeMaxInFlight(0)
	}

	var req *ClickhouseRequest
	{
		reader := bytes.NewBuffer(message.Body)
		err := msgp.Decode(reader, req)
		if err != nil {
			return err
		}
	}

	return q.local.Put(req)
}
