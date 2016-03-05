package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sethgrid/hart/service"
)

// DataIn is used in the http listenerHandler and main to accept work and pass it to services
var DataIn chan int

// movieSearchResult holds movies searches from themoviedb.com
type movieSearchResult struct {
	Results      []MovieData `json:"results"`
	TotalPages   int         `json:"total_pages"`
	TotalResults int         `json:"total_results"`
}

// castSearchResult holds cast searches from themoviedb.com
type castSearchResult struct {
	Results []Cast `json:"cast"`
}

// MovieData represents a single movie
// we can expand the fields if we want
// for example, we don't use `Adult`... yet.
type MovieData struct {
	Adult       bool   `json:"adult"`
	ID          int    `json:"id"`
	ReleaseDate string `json:"release_date"`
	Title       string `json:"title"`
}

// Cast holds the complete cast response
type Cast struct {
	CastID      int    `json:"cast_id"`
	Character   string `json:"character"`
	CreditID    string `json:"credit_id"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	ProfilePath string `json:"profile_path"`
}

// LogResult represents what we want to persist
type LogResult struct {
	Movie   string   `json:"movie_title"`
	Release string   `json:"release_date"`
	Cast    []string `json:"cast"`
}

type config struct {
	Port     int    `json:"port"`
	APIKey   string `json:"api-key"`
	AMQPURL1 string `json:"rabbitmq-url-1"`
	AMQPURL2 string `json:"rabbitmq-url-2"`
	AMQPURL3 string `json:"rabbitmq-url-3"`
}

func main() {
	var port int
	var amqpURL1, amqpURL2, amqpURL3 string
	var apiKey string
	var configPath string
	flag.IntVar(&port, "port", 9000, "set the port of the listener")
	flag.StringVar(&apiKey, "api-key", "3f73ec50653cfeda94a47c55105adb85", "set themoviedb api key")
	flag.StringVar(&amqpURL1, "rabbitmq-url-1", "amqp://guest:guest@localhost:5672/", "set the protocol, host, and port for rabbitmq year queue")
	flag.StringVar(&amqpURL2, "rabbitmq-url-2", "amqp://guest:guest@localhost:5672/", "set the protocol, host, and port for rabbitmq movie queue")
	flag.StringVar(&amqpURL3, "rabbitmq-url-3", "amqp://guest:guest@localhost:5672/", "set the protocol, host, and port for rabbitmq cast queue")
	flag.StringVar(&configPath, "config", "", "path to json config file")
	flag.Parse()

	// TODO either find a package or add some logic to let flags override config file
	if configPath != "" {
		log.Println("using config at %s, ignoring flags", configPath)
		data, err := ioutil.ReadFile(configPath)
		if err != nil {
			log.Fatal("unable to read config", err)
		}
		c := config{}
		err = json.Unmarshal(data, &c)
		if err != nil {
			log.Fatal("unable to marshal config", err)
		}
		log.Printf("%#v", c)
		port = c.Port
		apiKey = c.APIKey
		amqpURL1 = c.AMQPURL1
		amqpURL2 = c.AMQPURL2
		amqpURL3 = c.AMQPURL3
	}

	// init the chan that lets users submit a year
	DataIn = make(chan int)

	var err error

	// initialize the services.
	// A listens on year, and will pass work to movie
	// B listens on movie, and will pass work to cast
	// C listens on cast, and will log/write results
	a := service.New("A", amqpURL1, "year")
	b := service.New("B", amqpURL2, "movie")
	c := service.New("C", amqpURL3, "cast")

	// define what each service does with work it receives
	// i.e., each will parse incoming data, mutate it, and pass it on

	a.Receiver = func(data []byte) {
		year, err := strconv.Atoi(string(data))
		if err != nil {
			log.Println("unable to read year", err)
			return
		}
		movies := fetchMovies(apiKey, year)

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

		castResult := fetchCast(apiKey, movie.ID)
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

	// start all the services; waitgroup will block until
	// we can be sure that all services are started
	wg := &sync.WaitGroup{}
	wg.Add(3)
	go a.Start(wg)
	go b.Start(wg)
	go c.Start(wg)
	wg.Wait()

	// handle the submission of new `year` values to kick off jobs
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

	// start a listener to so we can submit new years for which to fetch movies
	log.Printf("starting listening on :%d/submit/{year}", port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", port), Router())
	if err != nil {
		log.Fatal(err)
	}
}

func Router() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/submit/{year}", listenHandler)
	return r
}

// listenHandler allows us to submit new work to our services
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
	log.Println("new request")

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf("Ok - going to search for movies in %v... Check logs for results.\n", year)))
}

// fetchMovies returns movies from a given year
func fetchMovies(apiKey string, year int) movieSearchResult {
	results := movieSearchResult{}
	resp, err := http.Get(fmt.Sprintf("https://api.themoviedb.org/3/discover/movie?primary_release_year=%d&sort_by=vote_average.desc&api_key=%s", year, apiKey))
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

// fetchCast returns cast results for a given movie
func fetchCast(apiKey string, movieID int) castSearchResult {
	results := castSearchResult{}
	resp, err := http.Get(fmt.Sprintf("https://api.themoviedb.org/3/movie/%d/credits?api_key=%s", movieID, apiKey))
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
