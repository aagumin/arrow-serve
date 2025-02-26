package iskander

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"log"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
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

func (f *flightServer) getAllocator() memory.Allocator {
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

	return &flight.SchemaResult{Schema: flight.SerializeSchema(recs[0].Schema(), f.getAllocator())}, nil
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
			Schema: flight.SerializeSchema(recs[0].Schema(), f.getAllocator()),
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
func (f *flightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// Инициализируем писатель и читатель Arrow Record
	writer := flight.NewRecordWriter(stream)
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	// Пробегаемся по входным данным
	for reader.Next() {
		record := reader.Record()
		schema := record.Schema()
		numCols := record.NumCols()

		// Создаём новый Builder для манипуляции данными
		builder := array.NewRecordBuilder(f.getAllocator(), schema)

		// Обрабатываем каждую колонку с числовыми данными
		for i := 0; i < int(numCols); i++ {
			col := record.Column(i)
			switch column := col.(type) {
			case *array.Int32:
				// Если это колонка int32, удваиваем значения
				b := array.NewInt32Builder(f.getAllocator())
				compute.Multiply()
				for _, val := range column.Int32Values() {
					b.Append(val * 2)
				}
				arr := b.NewArray().(*array.Int32)
				builder.Field(i).(*array.Int32Builder).AppendValues(arr.Int32Values(), nil)
				arr.Release()
				//default:
				//	// Для других типов колонок – просто копируем
				//	array.NewData(builder.Field(i), col)
			}
		}

		// Создаём новое преобразованное запись
		newRecord := builder.NewRecord()
		defer newRecord.Release()

		// Отправляем запись обратно клиенту
		if err := writer.Write(newRecord); err != nil {
			return status.Errorf(codes.Internal, "failed to write record: %v", err)
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

	flightServer := &flightServer{}
	//flight_server.SetAuthHandler(&iskander.SimpleAuth{})
	server.RegisterFlightService(flightServer)

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
