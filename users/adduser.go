package users

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/scratchdata/scratchdb/config"
	"github.com/scratchdata/scratchdb/servers"

	"github.com/cqroot/prompt"
	"github.com/cqroot/prompt/multichoose"

	"github.com/pelletier/go-toml/v2"
)

// Replica represents each replica item within a shard
type Replica struct {
	Priority int
	Host     string
	Port     int
}

// Shard represents each shard item which contains replicas
type Shard struct {
	Weight              int
	InternalReplication bool
	Replicas            []Replica
}

// Server represents the overall structure for the XML template
type Server struct {
	Name   string
	Secret string
	Shards []Shard
}

var clusterTemplate string = `
<clickhouse>
	<remote_servers>
		<{{.Name}}>
			<secret>{{.Secret}}</secret>
			{{range .Shards}}
			<shard>
				<internal_replication>true</internal_replication>
				<weight>1</weight>
				<internal_replication>{{.InternalReplication}}</internal_replication>
				{{range .Replicas}}
				<replica>
					<priority>1</priority>
					<host>{{.Host}}</host>
					<port>{{.Port}}</port>
				</replica>
				{{end}}
			</shard>
			{{end}}
		</{{.Name}}>
	</remote_servers>
</clickhouse>
`

type DefaultUserManager struct {
	clickhouseManager servers.ClickhouseManager
}

func NewDefaultUserManager(clickhouseManager servers.ClickhouseManager) *DefaultUserManager {
	return &DefaultUserManager{clickhouseManager: clickhouseManager}
}

const alphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// generatePassword creates a secure alphanumeric password of the given length.
func generatePassword(length int) (string, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	password := make([]byte, length)
	for i, v := range b {
		password[i] = alphanumericChars[v%byte(len(alphanumericChars))]
	}

	return string(password), nil
}

func filter(A, B []string) []string {
	bMap := make(map[string]bool)
	for _, b := range B {
		bMap[b] = true
	}

	result := []string{}
	for _, a := range A {
		if _, found := bMap[a]; !found {
			result = append(result, a)
		}
	}

	return result
}

func (m *DefaultUserManager) GetDBManager() servers.ClickhouseManager {
	return m.clickhouseManager
}

func (m *DefaultUserManager) generateServerConfig(clusterName string, secret string, shards []string, replicas []string) Server {
	rc := Server{
		Name:   clusterName,
		Secret: secret,
	}

	replicasPerShard := len(replicas) / len(shards)
	replicaIndex := 0
	for _, shard := range shards {
		s := Shard{}
		r := Replica{
			Host: shard,
		}
		s.Replicas = append(s.Replicas, r)

		for j := 0; j < replicasPerShard; j++ {
			r := Replica{
				Host: replicas[replicaIndex],
			}
			s.Replicas = append(s.Replicas, r)
			replicaIndex++
		}

		rc.Shards = append(rc.Shards, s)
	}

	return rc
}

func (m *DefaultUserManager) generateClickhouseXML(data Server) string {
	tmpl, err := template.New("xmlTemplate").Parse(clusterTemplate)
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}

// TODO: this should take a list of servers as input since we'll do this from the UI in the future
func (m *DefaultUserManager) AddUser(userIdentifier string) error {
	if userIdentifier == "" {
		return errors.New("Invalid user")
	}

	// TODO: check to see if user already exists

	log.Println("Adding user", userIdentifier)

	serverHosts := []string{}
	for _, host := range m.GetDBManager().GetServers() {
		serverHosts = append(serverHosts, host.GetHost())
	}

	shards, _ := prompt.New().Ask("Shards:").
		MultiChoose(
			serverHosts,
			multichoose.WithHelp(true),
		)

	hostsForReplicas := filter(serverHosts, shards)
	replicas, _ := prompt.New().Ask("Replicas:").
		MultiChoose(
			hostsForReplicas,
			multichoose.WithHelp(true),
		)

	// TODO: let the user be able to have some shards with replicas and
	// others without if they really want to
	if len(replicas)%len(shards) != 0 {
		log.Println("each shard must have the same number of replicas")
		os.Exit(1)
	}

	// Generate Clickhouse XML for new cluster
	clusterName, _ := generatePassword(16)
	dbName, _ := generatePassword(8)
	dbUser, _ := generatePassword(8)
	clusterSecret, _ := generatePassword(32)
	serverConfig := m.generateServerConfig(clusterName, clusterSecret, shards, replicas)
	fmt.Println(m.generateClickhouseXML(serverConfig))

	/*
		<!-- Make sure to set these configs for cluster management -->
		<clickhouse>
			<users>
				<default>
					<access_management>1</access_management>
					<named_collection_control>1</named_collection_control>
					<show_named_collections>1</show_named_collections>
					<show_named_collections_secrets>1</show_named_collections_secrets>
				</default>
			</users>
			<access_control_improvements>
				<select_from_system_db_requires_grant>1</select_from_system_db_requires_grant>
			</access_control_improvements>
		</clickhouse>
	*/
	dbPass, _ := generatePassword(32)
	fmt.Printf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s' ON CLUSTER %s;\n", dbUser, dbPass, clusterName)
	fmt.Printf("GRANT SELECT ON %s.* to %s WITH REPLACE OPTION ON CLUSTER %s;\n", dbName, dbUser, clusterName)

	fmt.Println()
	// Create ScratchDB user
	apiKey, _ := generatePassword(32)
	apiDetails := config.UserConfig{
		Name:       userIdentifier,
		DBCluster:  clusterName,
		DBName:     dbName,
		DBUser:     dbUser,
		DBPassword: dbPass,
		APIKey:     apiKey,
	}
	jsonData, _ := toml.Marshal(apiDetails)

	fmt.Println("[[users]]")
	fmt.Println(string(jsonData))

	return nil
}
