package service

import (
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

type Service struct {
	id string

	host string

	receiveChan chan []byte

	Receiver    func([]byte)
	listenTopic string

	ch *amqp.Channel
}

type Message struct {
	Topic string
	Data  []byte
}

func New(id string, host string, listenTopic string) *Service {
	return &Service{
		id:          id,
		host:        host,
		listenTopic: listenTopic,
		receiveChan: make(chan []byte),
	}
}

// func (s *Service) Close() error{
// 	s.conn.Close()
// 	s.ch.Close()
// }

func (s *Service) Start(wg *sync.WaitGroup) error {
	log.Println("starting", s.id)
	conn, err := amqp.Dial(s.host)

	failOnError(err, "unable to connecto to rabbit")

	defer conn.Close()

	s.ch, err = conn.Channel()

	failOnError(err, "unable to make channel to rabbit")

	defer s.ch.Close()

	_, err = s.ch.QueueDeclare(
		s.listenTopic, // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "unable to declare queue")
	log.Printf("%s listening on %s", s.id, s.listenTopic)

	msgs, err := s.ch.Consume(
		s.listenTopic, // queue
		"",            // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			if d.Body == nil {
				log.Println("** nil body")
				continue
			}
			log.Printf("Received a message: %s", d.Body)

			if s.Receiver == nil {
				log.Fatal("you must define a reciever function for", s.id)
			}

			s.Receiver(d.Body)
		}
	}()

	wg.Done()
	forever := make(chan bool)
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return nil
}

func (s *Service) Publish(m Message) error {
	log.Println("publishing to", m.Topic)
	if s.ch == nil {
		log.Println("no channel")
	}

	return s.ch.Publish(
		"",      // exchange
		m.Topic, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        m.Data,
		})
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
