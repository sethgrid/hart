// +build integration

package service_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/sethgrid/hart/service"
)

func TestWorkChain(t *testing.T) {
	var startVal int = 9
	var receivedVal int

	host := "amqp://guest:guest@localhost:5672/"
	a := service.New("A", host, "a-incoming")
	b := service.New("B", host, "b-incoming")

	a.Receiver = func(data []byte) {
		i, _ := strconv.Atoi(string(data))
		err := b.Publish(service.Message{Topic: "b-incoming", Data: []byte(strconv.Itoa(i * i))})
		if err != nil {
			t.Fatal("unable to publish to a-incoming", err)
		}
	}

	waitForReceiver := make(chan struct{})
	b.Receiver = func(data []byte) {
		i, _ := strconv.Atoi(string(data))
		receivedVal = i
		close(waitForReceiver)
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go a.Start(wg)
	go b.Start(wg)
	wg.Wait()

	// kick off _something_ that will publish to a-incoming
	err := a.Publish(service.Message{Topic: "a-incoming", Data: []byte(strconv.Itoa(startVal))})
	if err != nil {
		t.Fatal("unable to publish to a-incoming", err)
	}

	<-waitForReceiver

	if got, want := receivedVal, startVal*startVal; got != want {
		t.Errorf("number not processed through workers. got %d, want %d", got, want)
	}
}
