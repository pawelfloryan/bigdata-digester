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

	db.createTables()

	//db.insertData()

	db.selectData()

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

func (db *DB) insertData() error {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	/*
		`INSERT INTO youtube.youtube_stats
			VALUES (
			'id',
			'2019-01-01 00:00:00',
			'upload_date_str',
			'2019-01-01',
			'title',
			'uploader_id',
			'uploader',
			12,
			false,
			12,
			13,
			145,
			true,
			true,
			true,
			true,
			'description',
			[('call', 'content', 'subtitle', 'title', 'url')],
			[('text', 'url')],
			'uploader_badges',
			'video_badges'
		)`
	*/

	err := db.Conn.Exec(ctx, `
	INSERT INTO youtube.youtube_stats
	SETTINGS input_format_null_as_default = 1
	SELECT
    	id,
    	parseDateTimeBestEffortUSOrZero(toString(fetch_date)) AS fetch_date,
    	upload_date AS upload_date_str,
    	toDate(parseDateTimeBestEffortUSOrZero(upload_date::String)) AS upload_date,
    	ifNull(title, '') AS title,
    	uploader_id,
    	ifNull(uploader, '') AS uploader,
    	uploader_sub_count,
    	is_age_limit,
    	view_count,
    	like_count,
    	dislike_count,
    	is_crawlable,
    	has_subtitles,
    	is_ads_enabled,
    	is_comments_enabled,
    	ifNull(description, '') AS description,
    	rich_metadata,
    	super_titles,
    	ifNull(uploader_badges, '') AS uploader_badges,
    	ifNull(video_badges, '') AS video_badges
	FROM s3(
    	'https://clickhouse-public-datasets.s3.amazonaws.com/youtube/original/files/*.zst',
    	'JSONLines'
	)
	`)

	return err
}

func (db *DB) selectData() (driver.Rows, error) {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	data, err := db.Conn.Query(ctx, `SELECT * FROM youtube.youtube_stats_trimmed`)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (db *DB) createTables() error {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	err := db.Conn.Exec(ctx, `
	CREATE TABLE youtube.youtube_stats (
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
	)
		ENGINE = MergeTree()
		ORDER BY (uploader, upload_date)
	`)
	if err != nil {
		return err
	}

	err = db.Conn.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS youtube.youtube_stats_trimmed (
		title String,
		like_count Int64,
		uploader_sub_count Int64,
		dislike_count Int64,
		view_count Int64,
	) 
		ENGINE = AggregatingMergeTree()
		ORDER BY (title)
	`)
	if err != nil {
		return err
	}

	err = db.Conn.Exec(ctx,
		`CREATE MATERIALIZED VIEW IF NOT EXISTS youtube.youtube_stats_trimmed_mv
	TO youtube.youtube_stats_trimmed AS
	SELECT
		title,
		like_count,
		uploader_sub_count,
		dislike_count,
		view_count
	FROM youtube.youtube_stats
	GROUP BY
		title,
		like_count,
		uploader_sub_count,
		dislike_count,
		view_count
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
