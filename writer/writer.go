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
		WriteChannel chan ConnEntry
		DatabaseName string
	}

	YT struct {
		Id                  string   `json:"id"`
		Fetch_date          string   `json:"fetch_date"`
		Upload_date_str     string   `json:"upload_date_str"`
		Upload_date         string   `json:"upload_date"`
		Title               string   `json:"title"`
		Uploader_id         string   `json:"uploader_id"`
		Uploader            string   `json:"uploader"`
		Uploader_sub_count  string   `json:"uploader_sub_count"`
		Is_age_limit        bool     `json:"is_age_limit"`
		View_count          string   `json:"view_count"`
		Like_count          string   `json:"like_count"`
		Dislike_count       string   `json:"dislike_count"`
		Is_crawlable        bool     `json:"is_crawlable"`
		Has_subtitles       bool     `json:"has_subtitles"`
		Is_ads_enabled      bool     `json:"is_ads_enabled"`
		Is_comments_enabled bool     `json:"is_comments_enabled"`
		Description         string   `json:"description"`
		Rich_metadata       []string `json:"rich_metadata"`
		Super_titles        []string `json:"super_titles"`
		Uploader_badges     string   `json:"uploader_badges"`
		Video_badges        string   `json:"video_badges"`
	}

	ConnEntry struct {
		Id                  string          `ch:"id"`
		Fetch_date          string          `ch:"fetch_date"`
		Upload_date_str     string          `ch:"upload_date_str"`
		Upload_date         string          `ch:"upload_date"`
		Title               string          `ch:"title"`
		Uploader_id         string          `ch:"uploader_id"`
		Uploader            string          `ch:"uploader"`
		Uploader_sub_count  int64           `ch:"uploader_sub_count"`
		Is_age_limit        bool            `ch:"is_age_limit"`
		View_count          int64           `ch:"view_count"`
		Like_count          int64           `ch:"like_count"`
		Dislike_count       int64           `ch:"dislike_count"`
		Is_crawlable        bool            `ch:"is_crawlable"`
		Has_subtitles       bool            `ch:"has_subtitles"`
		Is_ads_enabled      bool            `ch:"is_ads_enabled"`
		Is_comments_enabled bool            `ch:"is_comments_enabled"`
		Description         string          `ch:"description"`
		Rich_metadata       [][]interface{} `ch:"rich_metadata"`
		Super_titles        [][]interface{} `ch:"super_titles"`
		Uploader_badges     string          `ch:"uploader_badges"`
		Video_badges        string          `ch:"video_badges"`
	}
)

func NewWriter(database Database, databaseName string) *Writer {
	return &Writer{
		Database:     database,
		Wg:           new(sync.WaitGroup),
		WriteChannel: make(chan ConnEntry),
		DatabaseName: databaseName,
	}
}

func (w *Writer) WriteBatch(conn driver.Conn) {
	ctx := context.Background()

	var data []ConnEntry
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO youtube.youtube_stats")

	for change := range w.WriteChannel {
		data = append(data, change)
		if err != nil {
			log.Panicln(err)
		}

		for _, item := range data {
			err := batch.AppendStruct(&item)
			if err != nil {
				log.Panicln(err)
			}
		}
		close(w.WriteChannel)
	}
	err = batch.Send()
	if err != nil {
		log.Panicln(err)
	}
}
