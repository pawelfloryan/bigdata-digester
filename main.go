package main

import (
	"bufio"
	"context"
	"fmt"
	"learning-clickhouse/writer"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	zerolog "learning-clickhouse/logger"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	jsoniter "github.com/json-iterator/go"
)

type (
	DB struct {
		Conn     driver.Conn
		selected string
		ctx      context.Context
	}
)

// GetConn implements Database.
func (db *DB) GetConn() driver.Conn {
	return db.Conn
}

// GetContext implements Database.
func (db *DB) GetContext() context.Context {
	return db.ctx
}

// QueryParameters implements Database.
func (db *DB) QueryParameters(params clickhouse.Parameters) context.Context {
	return clickhouse.Context(db.ctx, clickhouse.WithParameters(params))
}

func main() {
	var wg sync.WaitGroup
	ctx := context.Background()
	db, err := connect(ctx)

	if err != nil {
		println("Couldn't connect to the database")
		panic(err)
	}

	ctx = clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	db.Conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS youtube")

	db.createTables()

	var count uint64
	db.Conn.QueryRow(ctx, "SELECT count() FROM youtube.youtube_stats").Scan(&count)

	if count == 0 {
		jsonWriter := writer.NewWriter(db, "youtube")
		entryChannel := make(chan writer.YT)
		wg.Add(3)
		go func() {
			defer wg.Done()
			jsonWriter.WriteBatch(db.Conn)
		}()
		go func() {
			defer wg.Done()
			scanFile(entryChannel)
		}()
		go func() {
			defer wg.Done()
			parseConn(entryChannel, jsonWriter.WriteChannel)
		}()
		wg.Wait()
	}
}

func scanFile(entryChannel chan<- writer.YT) {
	logger := zerolog.GetLogger()
	file, err := os.Open("./youtube.json")
	if err != nil {
		log.Panicln(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var data writer.YT
		if len(scanner.Bytes()) < 1 {
			continue
		}
		if scanner.Err() != nil {
			log.Fatal("failed to scan")
			return
		}

		if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(scanner.Bytes(), &data); err != nil {
			logger.Err(err).Bytes("record", scanner.Bytes()).Msg("failed to unmarshal line from JSON")
			continue
		}

		entryChannel <- data
	}
	close(entryChannel)
}

func parseConn(entryChannel chan writer.YT, writeChannel chan<- writer.ConnEntry) {
	for e := range entryChannel {
		uploaderSubCount, err := strconv.ParseInt(e.Uploader_sub_count, 10, 64)
		if err != nil {
			panic(err)
		}
		viewCount, err := strconv.ParseInt(e.Uploader_sub_count, 10, 64)
		if err != nil {
			panic(err)
		}
		likeCount, err := strconv.ParseInt(e.Uploader_sub_count, 10, 64)
		if err != nil {
			panic(err)
		}
		dislikeCount, err := strconv.ParseInt(e.Uploader_sub_count, 10, 64)
		if err != nil {
			panic(err)
		}

		entry := &writer.ConnEntry{
			Id:                  e.Id,
			Fetch_date:          e.Fetch_date,
			Upload_date_str:     e.Upload_date_str,
			Upload_date:         e.Upload_date,
			Title:               e.Title,
			Uploader_id:         e.Uploader_id,
			Uploader:            e.Uploader,
			Uploader_sub_count:  uploaderSubCount,
			Is_age_limit:        e.Is_age_limit,
			View_count:          viewCount,
			Like_count:          likeCount,
			Dislike_count:       dislikeCount,
			Is_crawlable:        e.Is_crawlable,
			Has_subtitles:       e.Has_subtitles,
			Is_ads_enabled:      e.Is_ads_enabled,
			Is_comments_enabled: e.Is_comments_enabled,
			Description:         e.Description,
			Rich_metadata:       e.Rich_metadata,
			Super_titles:        e.Super_titles,
			Uploader_badges:     e.Uploader_badges,
			Video_badges:        e.Video_badges,
		}
		writeChannel <- *entry
	}
	close(writeChannel)
}

func printOutLists(data driver.Rows) {
	fmt.Println("Uploader likes subscriptions dislikes views")
	fmt.Println("-------------------------------------------")
	for data.Next() {
		var (
			uploader                                                  string
			like_count, uploader_sub_count, dislike_count, view_count int64
		)
		if err := data.Scan(
			&uploader,
			&like_count,
			&uploader_sub_count,
			&dislike_count,
			&view_count,
		); err != nil {
			log.Fatal(err)
		}
		fmt.Println(uploader, like_count, uploader_sub_count, dislike_count, view_count)
	}
}

func (db *DB) selectData() (driver.Rows, driver.Rows, error) {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	good, errGood := db.Conn.Query(ctx, `SELECT * FROM youtube.youtube_stats_trimmed ORDER BY total_uploader_sub_count DESC, total_dislike_count DESC LIMIT 10`)

	if errGood != nil {
		return nil, nil, errGood
	}

	bad, errBad := db.Conn.Query(ctx, `SELECT * FROM youtube.youtube_stats_trimmed ORDER BY total_uploader_sub_count ASC, total_dislike_count DESC LIMIT 10`)

	if errBad != nil {
		return nil, nil, errBad
	}

	return good, bad, nil
}

func (db *DB) createTables() error {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"database": db.selected,
	}))

	//The table which receives insert queries from the client
	//Materialized view takes data from this table
	err := db.Conn.Exec(ctx, `
	CREATE TABLE youtube.youtube_stats (
		id String,
		fetch_date String,
		upload_date_str String,
		upload_date String,
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

	//The table which receives data from the Materialized View
	//Materialized view puts data in this table
	err = db.Conn.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS youtube.youtube_stats_trimmed (
		uploader String,
		total_like_count AggregateFunction(sum, Int64),
		total_uploader_sub_count AggregateFunction(sum, Int64),
		total_dislike_count AggregateFunction(sum, Int64),
		total_view_count AggregateFunction(sum, Int64),
	) 
		ENGINE = AggregatingMergeTree()
		ORDER BY (uploader)
	`)
	if err != nil {
		return err
	}

	//The Materialized View takes data from the youtube_stats table and puts it inside the youtube_stats_trimmed table
	//It can also be formatted in some way if you want to
	//These actions all happen while executing inserts of data on the youtube_stats table
	err = db.Conn.Exec(ctx,
		`CREATE MATERIALIZED VIEW IF NOT EXISTS youtube.youtube_stats_trimmed_mv
	TO youtube.youtube_stats_trimmed AS
	SELECT
		uploader,
		sumState(y.like_count) as total_like_count,
		sumState(y.uploader_sub_count) as total_uploader_sub_count,
		sumState(y.dislike_count) as total_dislike_count,
		sumState(y.view_count) as total_view_count
	FROM youtube.youtube_stats y
	GROUP BY
		uploader
	`)
	if err != nil {
		return err
	}
	return err
}

func connect(ctx context.Context) (*DB, error) {
	time.Sleep(1000)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
	})

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return &DB{
		Conn:     conn,
		selected: "youtube",
	}, nil
}
