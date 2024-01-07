package api

import (
	"errors"
	"scratchdata/models"
	"scratchdata/models/postgrest"
	"scratchdata/pkg/destinations"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

func (i *API) PostgrestQuery(c *fiber.Ctx) error {

	apiKey := c.Locals("apiKey").(models.APIKey)

	// TODO: read-only vs read-write connections
	connectionSetting := i.db.GetDatabaseConnection(apiKey.DestinationID)

	if connectionSetting.ID == "" {
		return errors.New("No DB Connections set up")
	}

	// TODO: need some sort of local connection pool or storage
	connection := destinations.GetDestination(connectionSetting)
	if connection == nil {
		return errors.New("Destination " + connectionSetting.Type + " does not exist")
	}

	queryString := c.Context().URI().QueryString()

	parser := &postgrest.PostgrestParser{Buffer: string(queryString)}
	err := parser.Init()
	if err != nil {
		return err
	}

	err = parser.Parse()
	if err != nil {
		return err
	}

	log.Print(parser.SprintSyntaxTree())

	root := &postgrest.Node{}
	postgrest.PopulateAST(string(queryString), root, parser.AST())

	postgrestQuery := &postgrest.Postgrest{}
	postgrestQuery.FromAST(root)

	log.Trace().Interface("postgrest", postgrestQuery).Send()

	err = connection.QueryPostgrest(*postgrestQuery, c.Context().Response.BodyWriter())
	return err
}
