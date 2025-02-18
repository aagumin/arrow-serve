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
	conn, err := grpc.NewClient("localhost:15002", grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	err = stream.Send(&flight.HandshakeRequest{Payload: []byte("foobaz")})
	if err != nil {
		return
	}
	ds, err := client.ListFlights(context.Background(), &flight.Criteria{})
	if err != nil {
		fmt.Println(err)
	}
	recv, err := ds.Recv()
	if err != nil {
		return
	}

	fmt.Println(string(recv.GetSchema()))

	get, err := client.DoGet(context.Background(), &flight.Ticket{})
	if err != nil {
		return
	}
	data, err := get.Recv()
	if err != nil {
		return
	}

	fmt.Println(string(data.GetDataBody()))
}
