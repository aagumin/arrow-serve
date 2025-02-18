package iskander

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SimpleAuth struct{}

func (sa *SimpleAuth) Authenticate(c flight.AuthConn) error {
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

func (sa *SimpleAuth) IsValid(token string) (interface{}, error) {
	if token == "foobar" {
		return "foo", nil
	}
	return "", status.Error(codes.PermissionDenied, "invalid auth token")
}
