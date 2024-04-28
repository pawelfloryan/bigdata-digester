package writer

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
		db           Database
		wg           sync.WaitGroup
		writeChannel chan Data
		database     string
	}
)

func (w *Writer) WriteBatch() {
	conn := w.db.GetConn()
	defer conn.Close()

	//ctx := w.db.QueryParameters(clickhouse.Parameters{
	//	"database": w.database,
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
