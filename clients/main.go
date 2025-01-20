package main

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
)

func main() {
	conn, err := grpc.NewClient("localhost:15003", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.Handshake(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// ignore error handling here for brevity
	stream.Send(&flight.HandshakeRequest{Payload: []byte("baz")})
	exchange, _ := client.DoExchange(context.Background())
	data := []byte("foo")
	dt := flight.FlightData{DataBody: data}
	err = exchange.Send(&dt)
	resp, _ := stream.Recv()
	fmt.Println(string(resp.Payload))
}
