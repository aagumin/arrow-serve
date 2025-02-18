package iskander

import (
	"context"
	"fmt"
	"log"
	"sort"

	iskander "iskander/pkg/auth"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	Records     = make(map[string][]arrow.Record)
	RecordNames []string
)

func init() {
	Records["primitives"] = makePrimitiveRecords()

	for k := range Records {
		RecordNames = append(RecordNames, k)
	}
	sort.Strings(RecordNames)
}

type flightServer struct {
	mem memory.Allocator
	flight.BaseFlightServer
}

func (f *flightServer) getmem() memory.Allocator {
	if f.mem == nil {
		f.mem = memory.NewGoAllocator()
	}

	return f.mem
}

func (f *flightServer) GetSchema(_ context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid flight descriptor")
	}

	recs, ok := Records[in.Path[0]]
	if !ok {
		return nil, status.Error(codes.NotFound, "flight not found")
	}

	return &flight.SchemaResult{Schema: flight.SerializeSchema(recs[0].Schema(), f.getmem())}, nil
}

func (f *flightServer) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	expr := string(c.GetExpression())

	auth := ""
	authVal := flight.AuthFromContext(fs.Context())
	if authVal != nil {
		auth = authVal.(string)
	}

	for _, name := range RecordNames {
		if expr != "" && expr != name {
			continue
		}

		recs := Records[name]
		totalRows := int64(0)
		for _, r := range recs {
			totalRows += r.NumRows()
		}

		err := fs.Send(&flight.FlightInfo{
			Schema: flight.SerializeSchema(recs[0].Schema(), f.getmem()),
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorPATH,
				Path: []string{name, auth},
			},
			TotalRecords: totalRows,
			TotalBytes:   -1,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *flightServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	Records["test"] = makePrimitiveRecords()
	recs, ok := Records[string(tkt.GetTicket())]

	if !ok {
		return status.Error(codes.NotFound, "flight not found")
	}
	w := flight.NewRecordWriter(fs, ipc.WithSchema(recs[0].Schema()))
	for _, r := range recs {
		err := w.Write(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func Run(addr string) error {
	RootLogger()
	log.Println("Try starting server")

	server := flight.NewServerWithMiddleware([]flight.ServerMiddleware{InterceptorLoggerMW()})
	err := server.Init(addr)
	if err != nil {
		return fmt.Errorf("flight server initialization failed: %v", err)
	}

	flight_server := &flightServer{}
	flight_server.SetAuthHandler(&iskander.SimpleAuth{})
	server.RegisterFlightService(flight_server)

	// go func() {
	err = server.Serve()
	log.Println("Flight server serving")
	if err != nil {
		panic(fmt.Errorf("flight server failed: %v", err))
	}
	//}()
	defer server.Shutdown()

	return nil
}
