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

	db.createTable(ctx)

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

func (db *DB) createTable(ctx context.Context) error {
	err := db.Conn.Exec(ctx, `
	CREATE TABLE youtubeStats (
		id String,
		fetch_date DateTime,
		upload_date_str String,
		upload_date Date,
		title String,
		uploader_id String,
		uploader String,
		uploader_sub_count Int64,
		is_age_limit Bool,
		view_count Int64,
		like_count Int64,
		dislike_count Int64,
		is_crawlable Bool,
		has_subtitles Bool,
		is_ads_enabled Bool,
		is_comments_enabled Bool,
		description String,
		rich_metadata Array(Tuple(call String, content String, subtitle String, title String, url String)),
		super_titles Array(Tuple(text String, url String)),
		uploader_badges String,
		video_badges String
	) ENGINE = AggregatingMergeTree()
	ORDER BY (uploader, upload_date)
`)
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
