package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sethgrid/hart/service"
)

// global
var DataIn chan int

type movieSearchResult struct {
	Results      []MovieData `json:"results"`
	TotalPages   int         `json:"total_pages"`
	TotalResults int         `json:"total_results"`
}

type castSearchResult struct {
	Results []Cast `json:"cast"`
}

type MovieData struct {
	Adult       bool   `json:"adult"`
	ID          int    `json:"id"`
	ReleaseDate string `json:"release_date"`
	Title       string `json:"title"`
}

type Cast struct {
	CastID      int    `json:"cast_id"`
	Character   string `json:"character"`
	CreditID    string `json:"credit_id"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	ProfilePath string `json:"profile_path"`
}

func main() {
	// read configs
	// start listener service
	// start service_a
	// start service_b
	// block
	var port int
	var amqpURL string
	var apiKey string
	flag.IntVar(&port, "port", 9000, "set the port of the listener")
	flag.StringVar(&apiKey, "api_key", "3f73ec50653cfeda94a47c55105adb85", "set themoviedb api key")
	//flag.StringVar(&amqpURL, "rabbitmq_url", "amqp://guest:guest@localhost:5672/", "set the protocol, host, and port for rabbitmq")
	flag.StringVar(&amqpURL, "rabbitmq_url", "amqp://mark:password@localhost:5672/", "set the protocol, host, and port for rabbitmq")

	flag.Parse()

	DataIn = make(chan int, 5)

	r := mux.NewRouter()
	r.HandleFunc("/submit/{year}", listenHandler)

	var err error

	// a := service.New(amqpURL, "default", "year")
	// b := service.New(amqpURL, "year", "movie")
	// c := service.New(amqpURL, "movie", "cast")
	// d := service.New(amqpURL, "cast", "default")

	a := service.New("A", amqpURL, "default", "year")
	b := service.New("B", amqpURL, "year", "movie")
	c := service.New("C", amqpURL, "movie", "cast")
	d := service.New("D", amqpURL, "foo", "default")

	a.Receiver = func(data []byte) {
		log.Println("A got ", string(data))
		b.Publish(service.Message{Topic: "movie", Data: data})
	}

	b.Receiver = func(data []byte) {
		log.Println("B got ", string(data))
		err := c.Publish(service.Message{Topic: "cast", Data: data})
		if err != nil {
			log.Println("Error!!!", err)
		}
		log.Println("no error")
	}

	c.Receiver = func(data []byte) {
		log.Println("c got ", string(data))
		// d.Publish(service.Message{Topic: "file", Data: data})
	}

	d.Receiver = func(data []byte) {
		log.Println("D got ", string(data))
	}

	go a.Start()
	go b.Start()
	go b.Start()
	go d.Start()

	// let the services get started
	// TODO: make this not racy
	<-time.After(2 * time.Second)

	go func() {
		for {
			select {
			case year := <-DataIn:
				log.Printf("Got year %d, send to A", year)
				err := a.Publish(service.Message{Topic: "year", Data: []byte(strconv.Itoa(year))})
				if err != nil {
					log.Printf("unable to publish %v", err)
				}
			case <-time.After(10 * time.Second):
				log.Println("waiting for a year...")
			}
		}
	}()

	log.Printf("starting listening on :%d/submit/{year}", port)

	err = http.ListenAndServe(fmt.Sprintf(":%d", port), r)
	if err != nil {
		log.Fatal(err)
	}
}

func listenHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	year := vars["year"]
	i, err := strconv.Atoi(year)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"year must be an integer"}`))
		return
	}

	DataIn <- i

	w.Write([]byte(fmt.Sprintf("hi - going to search for %v", year)))
}

func fetchMovies(year int) movieSearchResult {
	// https://api.themoviedb.org/3/discover/movie?primary_release_year=2010&sort_by=vote_average.desc&api_key=3f73ec50653cfeda94a47c55105adb85
	return movieSearchResult{}
}

func fetchCast(movieID int) castSearchResult {
	// https://api.themoviedb.org/3/movie/42079/credits?api_key=3f73ec50653cfeda94a47c55105adb85
	return castSearchResult{}
}
