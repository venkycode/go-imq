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

func Test_GetMessageQueue(t *testing.T) {
	q1, err := goimq.NewMessageQueue[bool, int](
		goimq.QueueConfig[bool, int]{
			Name: "q1",
			ConsumerFn: func(i bool) (int, error) {
				return 2, nil
			},
		},
	)

	if err != nil {
		t.Error(err)
	}

	q2, err := goimq.NewMessageQueue[int, string](
		goimq.QueueConfig[int, string]{
			Name: "q2",
			ConsumerFn: func(i int) (string, error) {
				return "hello", nil
			},
		},
	)

	if err != nil {
		t.Error(err)
	}

	q, err := goimq.GetMessageQueue[bool, int]("q1")
	if err != nil {
		t.Error(err)
	}

	if q != q1 {
		t.Error("q1 not returned")
	}

	qq, err := goimq.GetMessageQueue[int, string]("q2")
	if err != nil {
		t.Error(err)
	}

	if qq != q2 {
		t.Error("q2 not returned")
	}

	_, err = goimq.GetMessageQueue[int, int]("q1")
	if err != goimq.ErrQueueTypeMismatch {
		t.Error("q1 returned")
	}

	_, err = goimq.GetMessageQueue[bool, string]("q2")
	if err != goimq.ErrQueueTypeMismatch {
		t.Error("q2 returned")
	}

	_, err = goimq.GetMessageQueue[int, int]("q3")
	if err != goimq.ErrQueueNotFound {
		t.Error("q3 returned")
	}

}
