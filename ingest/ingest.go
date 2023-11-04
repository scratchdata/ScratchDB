package ingest

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	apikeys "scratchdb/api_keys"
	"scratchdb/config"
	"scratchdb/util"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
	"github.com/jeremywohl/flatten"
	"github.com/oklog/ulid/v2"
	"github.com/spyzhov/ajson"
	"golang.org/x/crypto/acme/autocert"
)

type FileIngest struct {
	Config *config.Config

	app     *fiber.App
	writers map[string]*FileWriter
	apiKeys apikeys.APIKeys
}

func NewFileIngest(config *config.Config, apiKeyManager apikeys.APIKeys) FileIngest {
	i := FileIngest{
		Config:  config,
		apiKeys: apiKeyManager,
	}
	i.app = fiber.New()

	i.writers = make(map[string]*FileWriter)
	return i
}

func (i *FileIngest) Index(c *fiber.Ctx) error {
	return c.SendString("ok")
}

func (i *FileIngest) HealthCheck(c *fiber.Ctx) error {
	// Check if server has been manually marked as unhealthy
	_, err := os.Stat(i.Config.Ingest.HealthCheckPath)
	if !os.IsNotExist(err) {
		log.Println("Server marked as unhealthy")
		return fiber.ErrBadGateway
	}

	// Ensure we haven't filled up disk
	currentFreeSpace := util.FreeDiskSpace(i.Config.Ingest.DataDir)
	if currentFreeSpace <= uint64(i.Config.Ingest.FreeSpaceRequiredBytes) {
		log.Println("Out of disk, failing health check")
		return fiber.ErrBadGateway
	}

	// Ensure we can fetch and use API keys
	apiKeysHealthy := i.apiKeys.Healthy()
	if apiKeysHealthy != nil {
		log.Println(apiKeysHealthy)
		return fiber.ErrBadGateway
	}

	// TODO: ensure there are actual users available
	return c.SendString("ok")
}

func (i *FileIngest) getField(header string, query string, body string, c *fiber.Ctx) (string, string) {
	// First try to get value from header
	rc := utils.CopyString(c.Get(header))
	location := "header"

	// Then try to get if from query param
	if rc == "" {
		rc = utils.CopyString(c.Query(query))
		location = "query"
	}

	// Then try to get it from JSON body
	if body != "" && rc == "" {
		location = "body"
		root, err := ajson.Unmarshal(c.Body())
		if err != nil {
			return "", ""
		}

		bodyKey, err := root.GetKey(body)
		rc, _ = bodyKey.GetString()
	}

	if rc == "" {
		return "", ""
	}
	return rc, location
}

// TODO: Common pool of writers and uploaders across all API keys, rather than one per API key
// TODO: Start the uploading process independent of whether new data has been inserted for that API key
func (i *FileIngest) InsertData(c *fiber.Ctx) error {
	if c.QueryBool("debug", false) {
		rid := ulid.Make().String()
		log.Println(rid, "Headers", c.GetReqHeaders())
		log.Println(rid, "Body", string(c.Body()))
		log.Println(rid, "Query Params", c.Queries())
	}

	api_key, _ := i.getField("X-API-KEY", "api_key", "api_key", c)
	_, ok := i.apiKeys.GetDetailsByKey(api_key)
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	input := c.Body()

	// Ensure JSON is valid
	if !json.Valid(input) {
		return fiber.ErrBadRequest
	}

	table_name, table_location := i.getField("X-SCRATCHDB-TABLE", "table", "table", c)
	if table_name == "" {
		return fiber.NewError(fiber.StatusBadRequest, "You must specify a table name")
	}

	flattenAlgorithm, _ := i.getField("X-SCRATCHDB-FLATTEN", "flatten", "flatten", c)

	data_path := "$"
	if table_location == "body" {
		data_path = "$.data"
	}

	root, err := ajson.Unmarshal(input)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	x, err := root.JSONPath(data_path)
	if err != nil {
		return err
	}

	dir := filepath.Join(i.Config.Ingest.DataDir, api_key, table_name)

	// TODO: make sure this is atomic!
	writer, ok := i.writers[dir]
	if !ok {
		writer = NewFileWriter(
			dir,
			i.Config,
			filepath.Join("data", api_key, table_name),
			map[string]string{"api_key": api_key, "table_name": table_name},
		)
		i.writers[dir] = writer
	}

	if x[0].Type() == ajson.Array {
		objects, err := x[0].GetArray()
		if err != nil {
			return err
		}
		for _, o := range objects {

			if flattenAlgorithm == "explode" {
				flats, err := FlattenJSON(o.String(), nil, false)
				if err != nil {
					return err
				}

				for _, flat := range flats {
					err = writer.Write(flat)
					if err != nil {
						log.Println("Unable to write object", flat, err)
					}

				}

			} else {
				flat, err := flatten.FlattenString(o.String(), "", flatten.UnderscoreStyle)
				if err != nil {
					return err
				}
				err = writer.Write(flat)
				if err != nil {
					log.Println("Unable to write object", flat, err)
				}
			}
		}

	} else if x[0].Type() == ajson.Object {
		if flattenAlgorithm == "explode" {
			flats, err := FlattenJSON(x[0].String(), nil, false)
			if err != nil {
				return err
			}

			for _, flat := range flats {
				err = writer.Write(flat)
				if err != nil {
					log.Println("Unable to write object", flat, err)
				}

			}

		} else {
			flat, err := flatten.FlattenString(x[0].String(), "", flatten.UnderscoreStyle)
			if err != nil {
				return err
			}

			err = writer.Write(flat)
			if err != nil {
				return fiber.NewError(fiber.StatusBadRequest, err.Error())
			}
		}
	}

	return c.SendString("ok")
}

func (im *FileIngest) makeRequestToClickhouse(database string, user string, password string, sql string) (*http.Response, error) {
	url := im.Config.Clickhouse.Protocol + "://" + im.Config.Clickhouse.Host + ":" + im.Config.Clickhouse.HTTPPort

	var jsonStr = []byte(sql)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Clickhouse-User", user)
	req.Header.Set("X-Clickhouse-Key", password)
	req.Header.Set("X-Clickhouse-Database", database)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return resp, nil
}

func (im *FileIngest) renderResponseToHTML(resp *http.Response, c *fiber.Ctx) {
	md, _ := io.ReadAll(resp.Body)
	// create markdown parser with extensions
	extensions := parser.CommonExtensions | parser.AutoHeadingIDs | parser.NoEmptyLineBeforeBlock
	p := parser.NewWithExtensions(extensions)
	doc := p.Parse(md)

	// create HTML renderer with extensions
	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	opts := html.RendererOptions{Flags: htmlFlags}
	renderer := html.NewRenderer(opts)

	html := markdown.Render(doc, renderer)
	c.Set(fiber.HeaderContentType, fiber.MIMETextHTML)
	c.WriteString(`
	<style>
	table, tr, td, th {border: 1px solid; border-collapse:collapse}
	td,th{padding:3px;}
	</style>
	`)
	c.Write(html)
}

func (im *FileIngest) parseResponseToJSON(resp *http.Response, c *fiber.Ctx) error {
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

	c.WriteString("[")

	// Treat the output as a linked list of text fragments.
	// Each fragment could be a partial JSON line
	var nextIsPrefix = true
	var nextErr error = nil
	var nextLine []byte
	reader := bufio.NewReader(resp.Body)
	line, isPrefix, err := reader.ReadLine()

	for {
		// If we're at the end of our input, break
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Output the data
		c.Write(line)

		// Check to see whether we are at the last row by looking for EOF
		nextLine, nextIsPrefix, nextErr = reader.ReadLine()

		// If the next row is not an EOF, then output a comma. This is to avoid a
		// trailing comma in our JSON
		if !isPrefix && nextErr != io.EOF {
			c.WriteString(",")
		}

		// Equivalent of "currentPointer = currentPointer.next"
		line, isPrefix, err = nextLine, nextIsPrefix, nextErr
	}
	c.WriteString("]")

	return nil
}

func (im *FileIngest) query(database string, user string, password string, query string, format string) (*http.Response, error) {
	var ch_format string
	switch format {
	case "html":
		ch_format = "Markdown"
	case "json":
		ch_format = "JSONEachRow"
	default:
		ch_format = "JSONEachRow"
	}

	// Possibly use squirrel library here: https://github.com/Masterminds/squirrel
	sql := "SELECT * FROM (" + query + ") FORMAT " + ch_format
	// log.Println(sql)

	resp, err := im.makeRequestToClickhouse(database, user, password, sql)
	return resp, err
}

func (i *FileIngest) Query(c *fiber.Ctx) error {
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

	format := utils.CopyString(c.Query("format", "json"))
	api_key, _ := i.getField("X-API-KEY", "api_key", "", c)
	keyDetails, ok := i.apiKeys.GetDetailsByKey(api_key)
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	resp, err := i.query(keyDetails.GetDBName(), keyDetails.GetDBUser(), keyDetails.GetDBPassword(), query, format)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return fiber.NewError(fiber.StatusUnauthorized)
	} else if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return fiber.NewError(fiber.StatusBadRequest, string(msg))
	}

	switch format {
	case "html":
		i.renderResponseToHTML(resp, c)
		return nil
	default:
		err := i.parseResponseToJSON(resp, c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *FileIngest) ScratchRESTGETHandler(c *fiber.Ctx) error {
	api_key, _ := i.getField("X-API-KEY", "api_key", "", c)

	keyDetails, ok := i.apiKeys.GetDetailsByKey(api_key)
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	table_name := utils.CopyString(c.Params("table_name"))

	select_query_param := c.Query("select", "")
	field_names := strings.Split(select_query_param, ",")
	var filter_fields []string
	// By enclosing inside ` `, an identifier, with the same name as a keyword, can also be passed
	for i, field := range field_names {
		trimmed_field := strings.TrimSpace(field_names[i])
		if len(trimmed_field) != 0 {
			filter_fields = append(filter_fields, "`"+field+"`")
		}
	}
	field_string := strings.Join(filter_fields, ",")
	if field_string == "" {
		field_string = "*"
	}

	sql := fmt.Sprintf("SELECT %s from %s", field_string, table_name)

	resp, err := i.query(keyDetails.GetDBName(), keyDetails.GetDBUser(), keyDetails.GetDBPassword(), sql, "json")
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	defer resp.Body.Close()

	err = i.parseResponseToJSON(resp, c)
	if err != nil {
		return err
	}

	return nil
}

func (i *FileIngest) runSSL() {

	// Certificate manager
	m := &autocert.Manager{
		Prompt: autocert.AcceptTOS,
		// Replace with your domain
		HostPolicy: autocert.HostWhitelist(i.Config.SSL.Hostnames...),
		// Folder to store the certificates
		Cache: autocert.DirCache("./certs"),
	}

	// TLS Config
	cfg := &tls.Config{
		// Get Certificate from Let's Encrypt
		GetCertificate: m.GetCertificate,
		// By default NextProtos contains the "h2"
		// This has to be removed since Fasthttp does not support HTTP/2
		// Or it will cause a flood of PRI method logs
		// http://webconcepts.info/concepts/http-method/PRI
		NextProtos: []string{
			"http/1.1", "acme-tls/1",
		},
	}
	ln, err := tls.Listen("tcp", ":443", cfg)
	if err != nil {
		panic(err)
	}

	if err := i.app.Listener(ln); err != nil {
		log.Panic(err)
	}
}

func (i *FileIngest) Start() {
	// TODO: recover from non-graceful shutdown. What if there are files left on disk when we restart?

	i.app.Use(logger.New())

	i.app.Get("/", i.Index)
	i.app.Get("/healthcheck", i.HealthCheck)

	i.app.Post("/data", i.InsertData)
	i.app.Get("/query", i.Query)
	i.app.Post("/query", i.Query)

	i.app.Get("/scratchrest/:table_name", i.ScratchRESTGETHandler)

	if i.Config.SSL.Enabled {
		i.runSSL()
	} else {
		if err := i.app.Listen(":" + i.Config.Ingest.Port); err != nil {
			log.Panic(err)
		}
	}

}

func (i *FileIngest) Stop() error {
	fmt.Println("Running cleanup tasks...")

	// TODO: set readtimeout to something besides 0 to close keepalive connections
	err := i.app.Shutdown()
	if err != nil {
		log.Println(err)
	}

	// Closing writers
	for name, writer := range i.writers {
		log.Println("Closing writer", name)
		err := writer.Close()
		if err != nil {
			log.Println(err)
		}
	}

	return err
}
