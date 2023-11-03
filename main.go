package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	apikeys "scratchdb/api_keys"
	"scratchdb/config"
	"scratchdb/importer"
	"scratchdb/ingest"
	"scratchdb/users"

	"github.com/spf13/viper"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ingestCmd := flag.NewFlagSet("ingest", flag.ExitOnError)
	ingestConfig := ingestCmd.String("config", "config.toml", "")

	insertCmd := flag.NewFlagSet("insert", flag.ExitOnError)
	insertConfig := insertCmd.String("config", "config.toml", "")

	addUserCmd := flag.NewFlagSet("adduser", flag.ExitOnError)
	addUserName := addUserCmd.String("user", "", "")
	addUserConfig := addUserCmd.String("config", "config.toml", "")

	var configFile string

	if len(os.Args) < 2 {
		fmt.Println("expected ingest or insert subcommands")
		os.Exit(1)
	}

	// Flag for server or consumer mode
	switch os.Args[1] {
	case "ingest":
		ingestCmd.Parse(os.Args[2:])
		configFile = *ingestConfig
	case "insert":
		insertCmd.Parse(os.Args[2:])
		configFile = *insertConfig
	case "adduser":
		addUserCmd.Parse(os.Args[2:])
		configFile = *addUserConfig
	default:
		log.Println("Expected ingest or insert")
		os.Exit(1)
	}

	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	var C config.Config
	err = viper.Unmarshal(&C)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	var wg sync.WaitGroup

	var apiKeyManager apikeys.APIKeys
	if C.UsersJSON != "" {
		apiKeyManager = &apikeys.APIKeysFromFile{
			FileName: C.UsersJSON,
		}
	}

	switch os.Args[1] {
	case "ingest":
		i := ingest.NewFileIngest(&C, apiKeyManager)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		wg.Add(1)
		go func() {
			_ = <-c
			fmt.Println("Gracefully shutting down import...")
			_ = i.Stop()
			wg.Done()
		}()

		i.Start()
	case "insert":
		i := importer.NewImporter(&C, apiKeyManager)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		wg.Add(1)
		go func() {
			_ = <-c
			fmt.Println("Gracefully shutting down insert...")
			_ = i.Stop()
			wg.Done()
		}()

		i.Start()
	case "adduser":
		var userManager users.UserManager
		userManager = &users.DefaultUserManager{}

		err := userManager.AddUser(*addUserName)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	default:
		log.Println("Expected ingest or insert")
		os.Exit(1)
	}

	wg.Wait()

}
