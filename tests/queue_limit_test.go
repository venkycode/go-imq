package tests

import (
	"fmt"
	"testing"
	"time"

	goimq "github.com/venkycode/go-imq"
)

func Test_testMaxMessages(t *testing.T) {

	maxMessages := uint(5)
	maxWorkers := uint(1)
	newQueue, error := goimq.NewMessageQueue[int, int](goimq.QueueConfig[int, int]{
		Name: "test",
		ConsumerFn: func(i int) (int, error) {
			fmt.Println(i)
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
		if !ok && i <= int(maxMessages+maxWorkers) {
			t.Error("Push failed")
		}

		if ok && i > int(maxMessages+maxWorkers) {
			t.Error("Push succeeded")
		}
	}

}
