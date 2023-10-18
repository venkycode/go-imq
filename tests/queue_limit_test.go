package tests

import (
	"testing"
	"time"

	goimq "github.com/venkycode/go-imq"
)

func Test_maxMessages(t *testing.T) {

	maxMessages := uint(5)
	maxWorkers := uint(0)
	newQueue, error := goimq.NewMessageQueue[int, int](goimq.QueueConfig[int, int]{
		Name: "test",
		ConsumerFn: func(i int) (int, error) {
			time.Sleep(2000 * time.Second)
			return 0, nil
		},
		MaxMessages: &maxMessages,
		MaxWorkers:  &maxWorkers,
	})

	if error != nil {
		t.Error(error)
	}

	for i := 1; i <= 10; i++ {
		_, ok := newQueue.Push(i)
		if !ok && i <= int(maxMessages) {
			t.Error("Push failed")
		}

		if ok && i > int(maxMessages) {
			t.Error("Push succeeded")
		}
	}

}

func Test_maxWorkers(t *testing.T) {
	maxMessages := uint(2)
	maxWorkers := uint(5)

	newQueue, error := goimq.NewMessageQueue[int, int](goimq.QueueConfig[int, int]{
		Name: "test",
		ConsumerFn: func(i int) (int, error) {
			time.Sleep(2000 * time.Second)
			return 0, nil
		},
		MaxMessages: &maxMessages,
		MaxWorkers:  &maxWorkers,
	})

	if error != nil {
		t.Error(error)
	}

	time.Sleep(200 * time.Millisecond) // let the workers start

	for i := 1; i <= int(maxWorkers+5); i++ {
		_, ok := newQueue.Push(i)
		if !ok && i <= int(maxWorkers+maxMessages) {
			t.Error("Push failed")
		}

		if ok && i > int(maxWorkers+maxMessages) {
			t.Error("Push succeeded")
		}

		time.Sleep(100 * time.Millisecond) // let the workers pick up the messages
	}

}
