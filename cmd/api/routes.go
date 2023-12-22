package api

import (
	"fmt"

	"github.com/gofiber/contrib/fiberzerolog"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (a *API) defaultMiddleware(handlers ...fiber.Handler) []fiber.Handler {
	return append([]fiber.Handler{
		// check auth first, to avoid exposing server status to unauthorized users
		a.AuthMiddleware,

		a.EnabledMiddleware,
		a.ReadonlyMiddleware,
	}, handlers...)
}

func (a *API) InitializeAPIServer() error {
	app := fiber.New()
	a.app = app

	app.Use(fiberzerolog.New(fiberzerolog.Config{
		Logger: &log.Logger,
		Levels: []zerolog.Level{zerolog.ErrorLevel, zerolog.WarnLevel, zerolog.TraceLevel},
	}))

	a.app.Get("/query", a.defaultMiddleware(a.Query)...)
	a.app.Post("/query", a.defaultMiddleware(a.Query)...)
	a.app.Post("/data", a.defaultMiddleware(a.Insert)...)

	err := app.Listen(fmt.Sprintf(":%d", a.config.Port))
	if err != nil {
		return err
	}

	return nil
}
