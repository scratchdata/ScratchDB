package api

import (
	"errors"
	"net/http"

	"github.com/jeremywohl/flatten"
	"scratchdata/models"

	"github.com/gofiber/fiber/v2"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

const (
	TableNameHeader = "X-SCRATCHDB-TABLE"
	TableNameQuery  = "table"
	TableNameJson   = "table"

	FlattenHeader = "X-SCRATCHDB-FLATTEN"
	FlattenQuery  = "flatten"
	FlattenJson   = "flatten"
)

type DataSource int

const (
	Undefined DataSource = iota
	InHeader
	InQuery
	InBody
)

type DataTarget struct {
	Source DataSource
	Name   string
}

type Data struct {
	Source DataSource
	Value  string
}

func lookUp(c *fiber.Ctx, targets ...DataTarget) Data {
	for _, target := range targets {
		var value string
		switch target.Source {
		case Undefined:
			continue
		case InHeader:
			value = c.Get(target.Name)
		case InQuery:
			value = c.Query(target.Name)
		case InBody:
			value = gjson.GetBytes(c.Body(), target.Name).String()
		}
		if value != "" {
			return Data{
				Value:  value,
				Source: target.Source,
			}
		}
	}
	return Data{}
}

func (a *API) Insert(c *fiber.Ctx) error {
	if c.QueryBool("debug", false) {
		rid := ulid.Make().String()
		log.Debug().
			Str("request_id", rid).
			Interface("headers", c.GetReqHeaders()).
			Str("body", string(c.Body())).
			Interface("queryParams", c.Queries()).
			Msg("Incoming request")
	}

	body := c.Body()
	if !gjson.ValidBytes(body) {
		return fiber.NewError(http.StatusBadRequest, "invalid JSON")
	}

	// TODO: this block can be abstracted as we also use it for query
	apiKey := c.Locals("apiKey").(models.APIKey)

	// TODO: read-only vs read-write connections
	connectionSetting := a.db.GetDatabaseConnection(apiKey.DestinationID)
	if connectionSetting.ID == "" {
		return fiber.NewError(http.StatusUnauthorized, "no connection is set up")
	}

	flatAlgoData := lookUp(c,
		DataTarget{InHeader, FlattenHeader},
		DataTarget{InQuery, FlattenQuery},
		DataTarget{InBody, FlattenJson},
	)

	tableNameData := lookUp(c,
		DataTarget{InHeader, TableNameHeader},
		DataTarget{InQuery, TableNameQuery},
		DataTarget{InBody, TableNameJson},
	)
	if tableNameData.Source == Undefined || tableNameData.Value == "" {
		return fiber.NewError(http.StatusBadRequest, "missing required table field")
	}

	parsed := gjson.ParseBytes(body)
	if tableNameData.Source == InBody {
		if parsed = parsed.Get("data"); !parsed.Exists() {
			return fiber.NewError(http.StatusBadRequest, "missing required data field")
		}
	}

	var (
		lines []string
		err   error
	)
	if flatAlgoData.Value == "explode" {
		explodeJSON, explodeErr := ExplodeJSON(parsed)
		if explodeErr != nil {
			log.Err(explodeErr).Str("parsed", parsed.Raw).Msg("error exploding JSON")
			err = errors.Join(err, explodeErr)
		}
		lines = append(lines, explodeJSON...)
	} else {
		flat, err := flatten.FlattenString(
			parsed.Raw,
			"",
			flatten.UnderscoreStyle,
		)
		if err != nil {
			return fiber.NewError(http.StatusBadRequest, err.Error())
		}
		lines = append(lines, flat)
	}

	for _, line := range lines {
		writeErr := a.dataTransport.Write(connectionSetting.ID, tableNameData.Value, []byte(line))
		if writeErr != nil {
			err = errors.Join(err, writeErr)
		}
	}
	if err != nil {
		return fiber.NewError(http.StatusExpectationFailed, err.Error())
	}

	return c.SendString("ok")
}
