package goimq

import (
	"errors"
)

var ErrQueueFull = errors.New("queue is full")

type ConsumerFn[T any, R any] func(T) (R, error)

type MessageQueue[T any, R any] struct {
	config *QueueConfig[T, R]
	queue  chan T
}

type MessageState struct {
}

type QueueConfig[T any, R any] struct {
	Name        string
	ConsumerFn  ConsumerFn[T, R]
	MaxWorkers  *uint
	MaxMessages *uint
	MaxAttempts *uint
}

func DefaultQueueConfig[T any, R any]() QueueConfig[T, R] {
	defaultMaxWorkers := uint(10)
	defaultMaxMessages := uint(1000)
	defaltMaxAttempts := uint(3)
	return QueueConfig[T, R]{
		MaxWorkers:  &defaultMaxWorkers,
		MaxMessages: &defaultMaxMessages,
		MaxAttempts: &defaltMaxAttempts,
	}
}

func NewMessageQueue[T any, R any](config QueueConfig[T, R]) (*MessageQueue[T, R], error) {

	if config.ConsumerFn == nil {
		return nil, errors.New("consumer function is required")
	}

	if config.Name == "" {
		return nil, errors.New("queue name is required")
	}

	defaultConfig := DefaultQueueConfig[T, R]()

	if config.MaxWorkers == nil {
		config.MaxWorkers = defaultConfig.MaxWorkers
	}

	if config.MaxMessages == nil {
		config.MaxMessages = defaultConfig.MaxMessages
	}

	queue := make(chan T, *config.MaxMessages)
	mq := &MessageQueue[T, R]{
		config: &config,
		queue:  queue,
	}
	mq.start()

	return mq, nil
}

func (mq *MessageQueue[T, R]) start() {

	for i := uint(0); i < *mq.config.MaxWorkers; i++ {
		go func() {
			for {
				message := <-mq.queue
				mq.config.ConsumerFn(message)
			}
		}()
	}

}

func (mq *MessageQueue[T, R]) Push(message T) (ok bool) {
	select {
	case mq.queue <- message:
		return true
	default:
		return false
	}
}
