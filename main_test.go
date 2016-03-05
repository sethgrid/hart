package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListener(t *testing.T) {
	expectedYear := 1990
	req, err := http.NewRequest("GET", fmt.Sprintf("/submit/%d", expectedYear), nil)
	if err != nil {
		log.Fatal(err)
	}

	DataIn = make(chan int)
	w := httptest.NewRecorder()

	go Router().ServeHTTP(w, req)

	var year int

	select {
	case year = <-DataIn:
	}

	if year != expectedYear {
		t.Errorf("got %d, want %d", year, expectedYear)
	}

}
