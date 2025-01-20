package iskander

import (
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

type serverAuth struct{}

func (sa *serverAuth) Authenticate(c flight.AuthConn) error {
	in, err := c.Read()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Unauthenticated, "no auth info provided")
	}

	if err != nil {
		return status.Error(codes.FailedPrecondition, "error reading auth handshake")
	}

	// do something with in....
	fmt.Println(string(in))

	// send auth token back
	return c.Send([]byte("foobar"))
}

func (sa *serverAuth) IsValid(token string) (interface{}, error) {
	if token == "foobar" {
		return "foo", nil
	}
	return "", status.Error(codes.PermissionDenied, "invalid auth token")
}

func Run() error {
	server := flight.NewFlightServer()
	err := server.Init("localhost:15003")
	if err != nil {
		return fmt.Errorf("flight server initialization failed: %v", err)
	}
	svc := &flight.BaseFlightServer{}
	svc.SetAuthHandler(&serverAuth{})
	server.RegisterFlightService(svc)
	//go func() {
	err = server.Serve()
	if err != nil {
		panic(fmt.Errorf("flight server failed: %v", err))
	}
	//}()
	defer server.Shutdown()
	return nil
}
