package bq

import (
	"context"
	"errors"
	"io"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
)

type BigQueryConnection struct {
	JSONCredentials string `mapstructure:"json_credentials"`
}

func (s *BigQueryConnection) QueryJSON(query string, writer io.Writer) error {
	ctx := context.TODO()

	c, err := bigquery.NewClient(ctx, "*detect-project-id*", option.WithCredentialsJSON([]byte(s.JSONCredentials)))
	log.Error().Err(err).Send()

	// q := client.Query("select num from t1 where name = @user")
	// q.Parameters = []bigquery.QueryParameter{
	// 	{Name: "user", Value: "Elizabeth"},
	// }

	q := c.Query("SELECT * FROM `bigquery-public-data.faa.us_airports` LIMIT 500")
	// q.DisableQueryCache = true
	// q.DryRun = true
	log.Error().Err(err).Send()

	job, err := q.Run(ctx)
	log.Error().Err(err).Send()

	iterator, err := job.Read(ctx)
	log.Error().Err(err).Send()

	log.Debug().Any("schema", iterator.Schema).Send()

	var values []bigquery.Value
	for {
		err = iterator.Next(&values)
		if err != nil {
			log.Error().Err(err).Send()
			break
		}
		log.Error().Interface("values", values).Send()
	}

	status, err := job.Status(ctx)
	log.Error().Err(err).Send()

	// Figure out pricing based on slots and data processed
	log.Error().Interface("statistics", status.Statistics).Send()

	statistics := status.Statistics
	queryStats := statistics.Details.(*bigquery.QueryStatistics)

	total_slot_ms := queryStats.SlotMillis
	execution_time_ms := statistics.EndTime.Sub(statistics.StartTime).Milliseconds()

	average_slots_used := float64(total_slot_ms) / float64(execution_time_ms)
	bytesBilled := queryStats.TotalBytesBilled

	log.Trace().Float64("slots", average_slots_used).Int64("bytes_billed", bytesBilled).Send()

	return nil
}

func (s *BigQueryConnection) InsertBatchFromNDJson(table string, input io.ReadSeeker) error {
	return errors.New("Not Implemented")
}

func (s *BigQueryConnection) Close() error {
	return nil
}
