package iskander

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
)

func RootLogger() (*log.Logger, []logging.Option) {
	logger := log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)

	opts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
		// Add any other option (check functions starting with logging.With).
	}
	return logger, opts
}

// InterceptorLogger adapts standard Go logger to interceptor logger.
// This code is simple enough to be copied and not imported.
func InterceptorLogger(l *log.Logger) logging.Logger {
	return logging.LoggerFunc(func(_ context.Context, lvl logging.Level, msg string, fields ...any) {
		switch lvl {
		case logging.LevelDebug:
			msg = fmt.Sprintf("DEBUG :%v", msg)
		case logging.LevelInfo:
			msg = fmt.Sprintf("INFO :%v", msg)
		case logging.LevelWarn:
			msg = fmt.Sprintf("WARN :%v", msg)
		case logging.LevelError:
			msg = fmt.Sprintf("ERROR :%v", msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
		l.Println(append([]any{"msg", msg}, fields...))
	})
}

func InterceptorLoggerMW() flight.ServerMiddleware {
	logger, opts := RootLogger()
	mv := flight.ServerMiddleware{
		Unary: grpc.UnaryServerInterceptor(
			logging.UnaryServerInterceptor(InterceptorLogger(logger), opts...),
		),
		Stream: grpc.StreamServerInterceptor(
			logging.StreamServerInterceptor(InterceptorLogger(logger), opts...),
		),
	}
	return mv
}
