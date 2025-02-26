package main

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
	// Подключение к серверу Flight
	conn, err := grpc.NewClient("localhost:15002", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.Handshake(context.Background())
	defer stream.CloseSend()
	if err != nil {
		log.Fatal(err)
	}

	// Создаём контекст
	ctx := context.Background()

	// Инициализируем память
	pool := memory.NewGoAllocator()
	//defer pool.()

	// Создаём схему данных для записи (одна колонка int32)
	fields := []arrow.Field{
		{Name: "values", Type: arrow.PrimitiveTypes.Int32},
	}
	schema := arrow.NewSchema(fields, nil)

	// Генерируем исходную запись данных (например, числа от 1 до 5)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	int32Builder := builder.Field(0).(*array.Int32Builder)
	int32Builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil) // Исходные данные

	record := builder.NewRecord()
	defer record.Release()

	// Устанавливаем поток для DoExchange
	exchangeStream, err := client.DoExchange(ctx)
	if err != nil {
		log.Fatalf("Ошибка установки потока DoExchange: %v", err)
	}

	// Отправляем запись на сервер
	writer := flight.NewRecordWriter(exchangeStream)
	if err := writer.Write(record); err != nil {
		log.Fatalf("Ошибка записи данных в поток DoExchange: %v", err)
	}
	writer.Close()

	// Читаем обработанные записи
	reader, _ := flight.NewRecordReader(exchangeStream)
	for reader.Next() {
		processedRecord := reader.Record()
		defer processedRecord.Release()

		// Читаем значения из полученной записи
		column := processedRecord.Column(0).(*array.Int32)
		for i := 0; i < column.Len(); i++ {
			log.Printf("Получено значение: %d", column.Value(i))
		}
	}

	// Завершаем стрим
	if err := exchangeStream.CloseSend(); err != nil {
		log.Fatalf("Ошибка завершения потока DoExchange: %v", err)
	}
}
