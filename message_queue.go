package goimq

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

type ConsumerFn[T any, R any] func(T) (R, error)

type messageInternal[T any] struct {
	messageID string
	message   T
}

type MessageQueue[T any, R any] struct {
	config                *QueueConfig[T, R]
	queue                 chan messageInternal[T]
	messageProcessingInfo sync.Map
}

var messageQueueStore sync.Map

type MessageProcessingInfo[ReturnType any] struct {
	RemainingAttempts uint
	State             MessageProcessingState

	Result ReturnType
	Errors []error
}

type MessageProcessingState int

const (
	MessageStateQueued     MessageProcessingState = iota // still in the queue
	MessageStateProcessing                               // currently being processed
	MessageStateCompleted                                // processing completed (either successfully or after all retries)
)

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
	defaltMaxAttempts := uint(1)
	return QueueConfig[T, R]{
		MaxWorkers:  &defaultMaxWorkers,
		MaxMessages: &defaultMaxMessages,
		MaxAttempts: &defaltMaxAttempts,
	}
}

var ErrConsumerFnRequired = errors.New("consumer function is required")
var ErrQueueNameRequired = errors.New("queue name is required")

func NewMessageQueue[T any, R any](config QueueConfig[T, R]) (*MessageQueue[T, R], error) {

	if config.ConsumerFn == nil {
		return nil, ErrConsumerFnRequired
	}

	if config.Name == "" {
		return nil, ErrQueueNameRequired
	}

	defaultConfig := DefaultQueueConfig[T, R]()

	if config.MaxWorkers == nil {
		config.MaxWorkers = defaultConfig.MaxWorkers
	}

	if config.MaxMessages == nil {
		config.MaxMessages = defaultConfig.MaxMessages
	}

	if config.MaxAttempts == nil {
		config.MaxAttempts = defaultConfig.MaxAttempts
	}

	queue := make(chan messageInternal[T], *config.MaxMessages)
	mq := &MessageQueue[T, R]{
		config:                &config,
		queue:                 queue,
		messageProcessingInfo: sync.Map{},
	}
	mq.start()

	messageQueueStore.Store(config.Name, mq)

	return mq, nil
}

var ErrQueueNotFound = errors.New("queue not found")
var ErrQueueTypeMismatch = errors.New("queue type mismatch")

func GetMessageQueue[T any, R any](name string) (*MessageQueue[T, R], error) {
	mq, ok := messageQueueStore.Load(name)
	if !ok {
		return nil, ErrQueueNotFound
	}

	mqTyped, ok := mq.(*MessageQueue[T, R])
	if !ok {
		return nil, ErrQueueTypeMismatch
	}

	return mqTyped, nil

}

func (mq *MessageQueue[T, R]) start() {

	// Start workers
	for i := uint(0); i < *mq.config.MaxWorkers; i++ {
		go func() {
			for { // keep processing messages on each worker
				message := <-mq.queue
				messageState, ok := mq.GetMessageProcessingInfo(message.messageID)
				if !ok {
					continue
				}

				mq.processMessageWithRetries(message, messageState)
			}
		}()
	}

}

func (mq *MessageQueue[T, R]) processMessageWithRetries(messageInternal messageInternal[T], latestMessageState MessageProcessingInfo[R]) {
	if latestMessageState.RemainingAttempts == 0 {
		return
	}

	result, err := mq.config.ConsumerFn(messageInternal.message)
	latestMessageState.RemainingAttempts = latestMessageState.RemainingAttempts - 1
	latestMessageState.Result = result

	switch {
	case err == nil:
		latestMessageState.State = MessageStateCompleted // message processing completed successfully
		mq.messageProcessingInfo.Store(messageInternal.messageID, latestMessageState)
		return
	case err != nil && latestMessageState.RemainingAttempts == 0:
		latestMessageState.State = MessageStateCompleted // message processing completed after all retries
		latestMessageState.Errors = append(latestMessageState.Errors, err)
		mq.messageProcessingInfo.Store(messageInternal.messageID, latestMessageState)
		return
	case err != nil && latestMessageState.RemainingAttempts > 0:
		latestMessageState.State = MessageStateProcessing // message processing failed, but will be retried
		latestMessageState.Errors = append(latestMessageState.Errors, err)
		mq.messageProcessingInfo.Store(messageInternal.messageID, latestMessageState)
		mq.processMessageWithRetries(messageInternal, latestMessageState)
	}

}

func (mq *MessageQueue[T, R]) Push(message T) (messageID string, ok bool) {
	messageID = uuid.New().String()
	mq.messageProcessingInfo.Store(messageID, mq.initialMessageProcessingInfo())
	select {
	case mq.queue <- messageInternal[T]{messageID: messageID, message: message}:
		return messageID, true
	default:
		mq.messageProcessingInfo.Delete(messageID)
		return "", false
	}
}

func (mq *MessageQueue[T, R]) initialMessageProcessingInfo() MessageProcessingInfo[R] {
	return MessageProcessingInfo[R]{
		RemainingAttempts: *mq.config.MaxAttempts,
		State:             MessageStateQueued,
		Result:            zeroOf[R](),
		Errors:            nil,
	}
}

func (mq *MessageQueue[T, R]) GetMessageProcessingInfo(messageID string) (MessageProcessingInfo[R], bool) {
	info, ok := mq.messageProcessingInfo.Load(messageID)
	if !ok {
		return MessageProcessingInfo[R]{}, false
	}
	return info.(MessageProcessingInfo[R]), true
}

func zeroOf[T any]() T {
	var zero T
	return zero
}
