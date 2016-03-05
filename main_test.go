package main

import (
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListener(t *testing.T) {
	// url does not matter
	req, err := http.NewRequest("GET", "http://somedomain/app/", nil)
	if err != nil {
		log.Fatal(err)
	}

	DataIn = make(chan int)
	w := httptest.NewRecorder()

	listenHandler(w, req)

	// select {
	// case year := <-DataIn:
	// 	fmt.Printf("YEAR!!", year)
	// }

}
