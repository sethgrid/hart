# RabbitMQ Movie Queue

RabbitMQ Movie Queue spins up three RabbitMQ Queues, `year`, `movie`, and `cast`. It accepts new querys at `0.0.0.0:9000/submit/{year}` and proceeds to place that in the `year` queue where a worker will process it and find movies to place in the `movie` queue, where another worker will process those movies to find their cast and place that in the `cast` queue.

## Usage
Staring:
`$ go run main.go`

Submitting:
`$ curl localhost:9000/year/1990`

You can update the sample config or provide flags to override any defaults

## Tests
`$ go test`

To run the integration test that verifies that the workflow works with rabbit:
`$ go test ./... -tags integration`

## Starting RabbitMQ (Mac)
`$ rabbitmq-server`