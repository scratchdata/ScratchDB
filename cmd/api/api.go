package api

import (
	"os"
	"scratchdata/config"
	"scratchdata/pkg/database"
	"scratchdata/pkg/transport"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

type API struct {
	config        config.API
	db            database.Database
	dataTransport transport.DataTransport

	app *fiber.App
}

func NewAPIServer(config config.API, db database.Database, dataTransport transport.DataTransport) *API {
	rc := &API{
		config:        config,
		db:            db,
		dataTransport: dataTransport,
	}
	return rc
}

func (a *API) Start() error {
	log.Info().Msg("Starting API")

	err := os.MkdirAll(a.config.DataDir, os.ModePerm)
	if err != nil {
		log.Fatal().Err(err).Str("path", a.config.DataDir).Msg("Unable to create data ingest directory")
	}

	err = a.InitializeAPIServer()
	if err != nil {
		return err
	}

	return nil
}

func (a *API) Stop() error {
	log.Info().Msg("Stopping API")
	err := a.app.Shutdown()
	if err != nil {
		log.Error().Err(err).Msg("failed to stop server")
	}
	return nil
}
