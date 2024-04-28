package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
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
		database     Database
		wg           *sync.WaitGroup
		writeChannel chan Data
		databaseName string
	}
)

func NewWriter(database Database, databaseName string) *Writer {
	return &Writer{
		database:     database,
		wg:           new(sync.WaitGroup),
		writeChannel: make(chan Data),
		databaseName: databaseName,
	}
}

func (w *Writer) WriteBatch() {
	conn := w.database.GetConn()
	defer conn.Close()

	//ctx := w.database.QueryParameters(clickhouse.Parameters{
	//	"database": w.databaseName,
	//})

	file, err := os.Open("./youtube.json")
	if err != nil {
		print(err)
		return
	}
	defer file.Close()

	var scanner *bufio.Scanner
	scanner = bufio.NewScanner(file)
	print(scanner)

	for scanner.Scan() {
		fmt.Print(scanner.Bytes())
	}

	//batch, err := conn.PrepareBatch(ctx, "INSERT INTO {database:Identifier}.youtube_stats")

}
