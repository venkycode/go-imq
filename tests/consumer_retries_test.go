package tests

import (
	"errors"
	"testing"
	"time"

	goimq "github.com/venkycode/go-imq"
)

func passOnNthAttemptConsumerFnGenerator(n uint) func(string) (string, error) {
	triesRemaining := n
	return func(message string) (string, error) {

		if triesRemaining > 1 {
			triesRemaining--
			return "", errors.New("error")
		}
		return "success", nil
	}
}

func TestConsumerRetries(t *testing.T) {

	consumerFn := passOnNthAttemptConsumerFnGenerator(3)
	_, err := consumerFn("test")
	if err == nil {
		t.Errorf("ConsumerFn() error = %v, want %v", err, "error")
	}

	_, err = consumerFn("test")
	if err == nil {
		t.Errorf("ConsumerFn() error = %v, want %v", err, "error")
	}

	result, err := consumerFn("test")
	if err != nil {
		t.Errorf("ConsumerFn() error = %v", err)
	}
	if result != "success" {
		t.Errorf("ConsumerFn() result = %v, want %v", result, "success")
	}

	tests := []struct {
		name        string
		maxAttempts uint
	}{
		{
			name:        "1 attempt",
			maxAttempts: 1,
		},
		{
			name:        "2 attempts",
			maxAttempts: 2,
		},
		{
			name:        "3 attempts",
			maxAttempts: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumerFn := passOnNthAttemptConsumerFnGenerator(tt.maxAttempts)
			q, err := goimq.NewMessageQueue[string, string](goimq.QueueConfig[string, string]{
				Name:        "test",
				ConsumerFn:  consumerFn,
				MaxAttempts: &tt.maxAttempts,
			})
			if err != nil {
				t.Errorf("NewMessageQueue() error = %v", err)
				return
			}
			messageID, ok := q.Push("test-1")
			if !ok {
				t.Errorf("Push() error")
				return
			}

			// Wait for the message to be processed
			time.Sleep(100 * time.Millisecond)

			// Check the message state
			message, ok := q.GetMessageProcessingInfo(messageID)
			if !ok {
				t.Errorf("GetMessageProcessingInfo() error")
				return
			}
			if message.State != goimq.MessageStateCompleted {
				t.Errorf("message state = %v, want %v", message.State, goimq.MessageStateCompleted)
			}

			// Check the message result
			if len(message.Errors) != int(tt.maxAttempts)-1 {
				t.Errorf("message errors = %v, want %v", len(message.Errors), tt.maxAttempts-1)
			}

		})
	}

}
