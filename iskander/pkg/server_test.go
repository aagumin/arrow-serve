package iskander

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDoExchange(t *testing.T) {
	tests := []struct {
		name       string
		input      []int32
		expect     []int32
		expectCode codes.Code
	}{
		{
			name:       "doubleValues",
			input:      []int32{1, 2, 3},
			expect:     []int32{2, 4, 6},
			expectCode: codes.OK,
		},
		{
			name:       "emptyValues",
			input:      []int32{},
			expect:     []int32{},
			expectCode: codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocator := memory.NewGoAllocator()
			server := &flightServer{mem: allocator}
			ctx := context.Background()

			stream := &mockDoExchangeStream{
				writeChan: make(chan arrow.Record, 1),
				readChan:  make(chan arrow.Record, 1),
				ctx:       ctx,
			}

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "col1", Type: arrow.PrimitiveTypes.Int32},
			}, nil)
			builder := array.NewRecordBuilder(allocator, schema)
			for _, val := range tt.input {
				builder.Field(0).(*array.Int32Builder).Append(val)
			}
			record := builder.NewRecord()
			defer record.Release()

			stream.readChan <- record
			close(stream.readChan)

			err := server.DoExchange(stream)

			if tt.expectCode != codes.OK {
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, tt.expectCode, st.Code())
				return
			}

			require.NoError(t, err)

			select {
			case outRecord := <-stream.writeChan:
				defer outRecord.Release()
				outCol := outRecord.Column(0).(*array.Int32)
				require.Equal(t, tt.expect, outCol.Int32Values())
			default:
				t.Fatalf("No record was written to the stream")
			}
		})
	}
}

type mockDoExchangeStream struct {
	flight.FlightService_DoExchangeServer
	writeChan chan arrow.Record
	readChan  chan arrow.Record
	ctx       context.Context
}

func (m *mockDoExchangeStream) Send(record arrow.Record) error {
	m.writeChan <- record
	return nil
}

func (m *mockDoExchangeStream) Recv() (arrow.Record, error) {
	record, ok := <-m.readChan
	if !ok {
		return nil, status.Error(codes.OutOfRange, "no more records")
	}
	return record, nil
}

func (m *mockDoExchangeStream) Context() context.Context {
	return m.ctx
}
