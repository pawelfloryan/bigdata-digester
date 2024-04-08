package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DB struct {
	Conn     driver.Conn
	selected string
}

func main() {
	db, err := connect()
	if err != nil {
		panic((err))
	}

	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	db.Conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS youtube")
	db.Conn.Exec(ctx, "USE youtube")

	db.createTable()

	/*
		for rows.Next() {
			var (
				id, fetch_date, upload_date, title, uploader_id, uploader, description string
			)
			if err := rows.Scan(
				&id,
				&fetch_date,
				&upload_date,
				&title,
				&uploader_id,
				&uploader,
				&description,
			); err != nil {
				log.Fatal(err)
			}
			log.Println(id, fetch_date, upload_date, title, uploader_id, uploader, description)
		}
	*/
}

func (db *DB) createTable() error {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))
	err := db.Conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS youtubeStats AS s3('https://clickhouse-public-datasets.s3.amazonaws.com/youtube/original/files/*.zst','JSONLines');")
	if err != nil {
		return err
	}
	return err
}

func connect() (*DB, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:19000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
	})

	if err != nil {
		return nil, err
	}

	return &DB{
		Conn:     conn,
		selected: "youtube",
	}, nil
}
