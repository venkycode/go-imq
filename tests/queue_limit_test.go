package tests

import (
	"testing"
	"time"

	goimq "github.com/venkycode/go-imq"
)

func Test_testMaxMessages(t *testing.T) {

	five := uint(5)

	newQueue, error := goimq.NewMessageQueue[int, int](goimq.QueueConfig[int, int]{
		Name: "test",
		ConsumerFn: func(int) (int, error) {
			time.Sleep(10 * time.Second)
			return 0, nil
		},
		MaxMessages: &five,
	})

	if error != nil {
		t.Error(error)
	}

	for i := 0; i < 10; i++ {
		ok := newQueue.Push(i)
		if !ok && i < 5 {
			t.Error("Push failed")
		}

		if ok && i >= 5 {
			t.Error("Push succeeded")
		}
	}

}
