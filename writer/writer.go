package writer

import (
	"context"
	"log"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type (
	Database interface {
		GetConn() driver.Conn
		GetContext() context.Context
		QueryParameters(clickhouse.Parameters) context.Context
	}

	Data any

	Writer struct {
		Database     Database
		Wg           *sync.WaitGroup
		WriteChannel chan Data
		DatabaseName string
	}
)

func NewWriter(database Database, databaseName string) *Writer {
	return &Writer{
		Database:     database,
		Wg:           new(sync.WaitGroup),
		WriteChannel: make(chan Data),
		DatabaseName: databaseName,
	}
}

func (w *Writer) WriteBatch(conn driver.Conn) {
	w.Wg.Add(1)
	go func() {
		//conn := w.database.GetConn()

		//ctx := w.database.QueryParameters(clickhouse.Parameters{
		//	"database": w.databaseName,
		//})

		ctx := context.Background()

		var data []Data
		print(<-w.WriteChannel)

		for change := range w.WriteChannel {
			data = append(data, change)
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO youtube.youtube_stats")
			if err != nil {
				log.Panicln(err)
			}

			for _, item := range data {
				err := batch.AppendStruct(item)
				if err != nil {
					log.Panicln(err)
				}
			}

			err = batch.Send()
			if err != nil {
				log.Panicln(err)
			}
		}

		w.Wg.Done()
	}()
	close(w.WriteChannel)
}
