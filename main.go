package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
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

type LogResult struct {
	Movie   string   `json:"movie_title"`
	Release string   `json:"release_date"`
	Cast    []string `json:"cast"`
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

	// start up queues
	// queues := []string{"year", "movie", "cast"}
	//    for _, queue := range queues{

	//    }

	r := mux.NewRouter()
	r.HandleFunc("/submit/{year}", listenHandler)

	var err error

	a := service.New("A", amqpURL, "year")
	b := service.New("B", amqpURL, "movie")
	c := service.New("C", amqpURL, "cast")

	a.Receiver = func(data []byte) {
		year, err := strconv.Atoi(string(data))
		if err != nil {
			log.Println("unable to read year", err)
			return
		}
		movies := fetchMovies(year)

		for _, movie := range movies.Results {
			mData, err := json.Marshal(movie)
			if err != nil {
				log.Println("unable to marshal movie information", err)
				return
			}

			err = b.Publish(service.Message{Topic: "movie", Data: mData})
			if err != nil {
				log.Println("unable to publish to movie topic", err)
			}
		}
	}

	b.Receiver = func(data []byte) {
		movie := &MovieData{}
		err := json.Unmarshal(data, &movie)
		if err != nil {
			log.Println("unable to unmarshal data", err)
			return
		}

		compiledInfo := &LogResult{}
		compiledInfo.Movie = movie.Title
		compiledInfo.Release = movie.ReleaseDate
		compiledInfo.Cast = make([]string, 0)

		castResult := fetchCast(movie.ID)
		for _, cast := range castResult.Results {
			compiledInfo.Cast = append(compiledInfo.Cast, cast.Name)
		}

		b, err := json.Marshal(compiledInfo)
		if err != nil {
			log.Println("unable to marshal compiled movie info", err)
			return
		}

		err = c.Publish(service.Message{Topic: "cast", Data: b})
		if err != nil {
			log.Println("error publishing to cast", err)
		}
	}

	c.Receiver = func(data []byte) {
		MovieCast := &LogResult{}
		err := json.Unmarshal(data, MovieCast)
		if err != nil {
			log.Println("unable to unmarshal movie cast", err)
			return
		}

		log.Printf("Movie Info: %s (%s) - %d cast members", MovieCast.Movie, MovieCast.Release, len(MovieCast.Cast))
	}

	go a.Start()
	go b.Start()
	go c.Start()

	// let the services get started
	// TODO: make this not racy
	<-time.After(1 * time.Second)

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
	results := movieSearchResult{}
	resp, err := http.Get(fmt.Sprintf("https://api.themoviedb.org/3/discover/movie?primary_release_year=%d&sort_by=vote_average.desc&api_key=3f73ec50653cfeda94a47c55105adb85", year))
	if err != nil {
		log.Println("unable to get movie results", err)
		return results
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("unable to read search body", err)
		return results
	}

	err = json.Unmarshal(data, &results)
	if err != nil {
		log.Println("unable to unmarshal movie search", err)
		return results
	}

	return results
}

func fetchCast(movieID int) castSearchResult {
	results := castSearchResult{}
	resp, err := http.Get(fmt.Sprintf("https://api.themoviedb.org/3/movie/%d/credits?api_key=3f73ec50653cfeda94a47c55105adb85", movieID))
	if err != nil {
		log.Println("unable to get cast results", err)
		return results
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("unable to read cast search body", err)
		return results
	}

	err = json.Unmarshal(data, &results)
	if err != nil {
		log.Println("unable to unmarshal cast search", err)
		return results
	}

	return results
}
