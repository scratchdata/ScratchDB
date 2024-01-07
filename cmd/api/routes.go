package api

import (
	"fmt"

	"github.com/gofiber/contrib/fiberzerolog"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (a *API) InitializeAPIServer() error {
	app := fiber.New()
	a.app = app

	app.Use(fiberzerolog.New(fiberzerolog.Config{
		Logger: &log.Logger,
		Levels: []zerolog.Level{zerolog.ErrorLevel, zerolog.WarnLevel, zerolog.TraceLevel},
	}))

	// Initialize default config
	app.Use(cors.New())

	a.app.Get("/healthcheck", a.AuthMiddleware, a.HealthCheck)
	a.app.Get("/query", a.AuthMiddleware, a.Query)
	a.app.Post("/query", a.AuthMiddleware, a.Query)
	a.app.Get("/tables", a.AuthMiddleware, a.Tables)
	a.app.Post("/data", a.AuthMiddleware, a.Insert)

	postgrest := a.app.Group("/postgrest")
	postgrest.Get("/:table", a.AuthMiddleware, a.PostgrestQuery)

	err := app.Listen(fmt.Sprintf(":%d", a.config.Port))
	if err != nil {
		return err
	}

	return nil
}
