package main

import (
	"bytes"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nikepan/go-datastructures/queue"
	"github.com/nsqio/go-nsq"
	"github.com/tinylib/msgp/msgp"
)

const (
	MaxInFlight = 8
)

var (
	ReservedLocalQueueSize = MaxInFlight*runtime.NumCPU()*2 + runtime.NumCPU()*2
)

type Queue struct {
	local               *queue.Queue
	localQueueCapacity  int64
	consumer            *nsq.Consumer
	producers           []*nsq.Producer
	nsqConfig           *nsq.Config
	nsqlookupdAddresses []string
	nsqdAddresses       []string

	topic   string
	channel string

	currentProducer int32

	wg sync.WaitGroup
}

func NewQueue(capacity int64) *Queue {
	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxInFlight = 0
	nsqConfig.MaxAttempts = 32

	instance := &Queue{
		local:              queue.New(capacity),
		localQueueCapacity: capacity,
		nsqConfig:          nsqConfig,
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
		if q.local.Len() == 0 {
			q.consumer.ChangeMaxInFlight(MaxInFlight)
		}
	}
	return data, err
}

func (q *Queue) Ack(n uint64) {
	var count int = int(n)

	for i := 0; i < count; i++ {
		q.wg.Done()
	}
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
			q.wg.Add(1)
			if err := q.local.Put(r); err != nil {
				q.wg.Done()
			}
			return err
		}
	}
	return nil
}

func (q *Queue) Stop() {
	q.wg.Wait()
	q.consumer.Stop()
}

func (q *Queue) init() {
	consumer, err := nsq.NewConsumer(q.topic, q.channel, q.nsqConfig)
	if err != nil {
		panic(err)
	}
	consumer.AddConcurrentHandlers(nsq.HandlerFunc(q.handleNsqMessage), runtime.NumCPU())
	if len(q.nsqlookupdAddresses) > 0 {
		err = consumer.ConnectToNSQLookupds(q.nsqlookupdAddresses)
		if err != nil {
			panic(err)
		}
	} else {
		err = consumer.ConnectToNSQDs(q.nsqdAddresses)
		if err != nil {
			panic(err)
		}
	}

	q.consumer = consumer

	for _, addr := range q.nsqdAddresses {
		producer, err := nsq.NewProducer(addr, q.nsqConfig)
		if err != nil {
			panic(err)
		}
		q.producers = append(q.producers, producer)
	}
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
	if (q.localQueueCapacity - q.local.Len()) < int64(ReservedLocalQueueSize) {
		q.consumer.ChangeMaxInFlight(0)
	}

	var req ClickhouseRequest
	{
		reader := bytes.NewBuffer(message.Body)
		err := msgp.Decode(reader, &req)
		if err != nil {
			return err
		}
	}

	q.wg.Add(1)
	if err := q.local.Put(&req); err != nil {
		q.wg.Done()
		return err
	}
	return nil
}
