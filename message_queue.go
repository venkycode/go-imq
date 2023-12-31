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
	Error  error
}

type MessageProcessingState int

const (
	MessageStateQueued MessageProcessingState = iota
	MessageStateProcessing
	MessageStateCompleted
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

// GetMessageQueueInferType is a helper function to get a message queue without having to specify the type parameters.
// Just pass the consumer function you used to create the queue.
func GetMessageQueueInferType[T any, R any](name string, consumerFn ConsumerFn[T, R]) (*MessageQueue[T, R], error) {
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

	for i := uint(0); i < *mq.config.MaxWorkers; i++ {
		go func() {
			for {
				mq.processMessageWithRetries(*mq.config.MaxAttempts, <-mq.queue)
			}
		}()
	}

}

func (mq *MessageQueue[T, R]) processMessageWithRetries(remainingAttempts uint, messageInternal messageInternal[T]) {
	// Guard clause to prevent infinite loop
	if remainingAttempts == 0 {
		return
	}
	mq.messageProcessingInfo.Store(messageInternal.messageID, MessageProcessingInfo[R]{
		RemainingAttempts: remainingAttempts,
		State:             MessageStateProcessing,
		Result:            zeroOf[R](),
		Error:             nil,
	})

	result, err := mq.config.ConsumerFn(messageInternal.message)
	remainingAttempts--

	switch {
	case err == nil:
		state := MessageStateCompleted
		mq.messageProcessingInfo.Store(messageInternal.messageID, MessageProcessingInfo[R]{
			RemainingAttempts: remainingAttempts,
			State:             state,
			Result:            result,
			Error:             nil,
		})
		return
	case err != nil && remainingAttempts == 0:
		state := MessageStateCompleted
		mq.messageProcessingInfo.Store(messageInternal.messageID, MessageProcessingInfo[R]{
			RemainingAttempts: remainingAttempts,
			State:             state,
			Result:            result,
			Error:             err,
		})
		return
	case err != nil && remainingAttempts > 0:
		state := MessageStateProcessing
		mq.messageProcessingInfo.Store(messageInternal.messageID, MessageProcessingInfo[R]{
			RemainingAttempts: remainingAttempts,
			State:             state,
			Result:            zeroOf[R](),
			Error:             nil,
		})
		mq.processMessageWithRetries(remainingAttempts, messageInternal)
	}

}

func (mq *MessageQueue[T, R]) Push(message T) (messageID string, ok bool) {
	messageID = uuid.New().String()
	select {
	case mq.queue <- messageInternal[T]{messageID: messageID, message: message}:
		mq.messageProcessingInfo.Store(messageID, MessageProcessingInfo[R]{
			RemainingAttempts: *mq.config.MaxAttempts,
			State:             MessageStateQueued,
			Result:            zeroOf[R](),
			Error:             nil,
		})
		return messageID, true
	default:
		return "", false
	}
}

func (mq *MessageQueue[T, R]) GetMessageProcessingInfo(messageID string) (MessageProcessingInfo[R], bool) {
	info, ok := mq.messageProcessingInfo.Load(messageID)
	if !ok {
		return MessageProcessingInfo[R]{}, false
	}
	return info.(MessageProcessingInfo[R]), true
}
