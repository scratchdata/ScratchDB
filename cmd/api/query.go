package api

import (
	"errors"
	"scratchdata/models"
	"scratchdata/pkg/destinations"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
)

func (i *API) Query(c *fiber.Ctx) error {
	query := utils.CopyString(c.Query("q"))

	if c.Method() == "POST" {
		payload := struct {
			Query string `json:"query"`
		}{}

		if err := c.BodyParser(&payload); err != nil {
			return err
		}

		query = payload.Query
	}

	apiKey := c.Locals("apiKey").(models.APIKey)

	// TODO: read-only vs read-write connections
	connectionSetting := i.db.GetDatabaseConnection(apiKey.DestinationID)

	if connectionSetting.ID == "" {
		return errors.New("No DB Connections set up")
	}

	// TODO: need some sort of local connection pool or storage
	connection, err := destinations.GetDestination(connectionSetting)
	if err != nil {
		return errors.New("Destination " + connectionSetting.Type + " does not exist")
	}

	// TODO: use a buffered pipe of some sort to stream results
	// https://github.com/gofiber/fiber/issues/1034
	// https://stackoverflow.com/questions/68778961/how-to-configure-the-buffer-size-for-http-responsewriter

	switch c.Query("format", "json") {
	case "json":
		c.Set("Content-type", "application/json")
		err = connection.QueryJSON(query, c.Context().Response.BodyWriter())
	default:
		c.Set("Content-type", "application/json")
		err = connection.QueryJSON(query, c.Context().Response.BodyWriter())
	}

	return err
}

func (i *API) Tables(c *fiber.Ctx) error {
	type t struct {
		Name string `json:"name"`
	}
	return c.JSON([]t{t{Name: "log"}, t{Name: "test_table"}})
}
