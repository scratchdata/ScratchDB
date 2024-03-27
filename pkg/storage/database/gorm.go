package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/scratchdata/scratchdata/pkg/config"
	"gorm.io/driver/postgres"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Gorm struct {
	conf                config.Database
	destinations        []config.Destination
	apiKeyToDestination map[string]int64
	adminAPIKeys        []config.APIKey
	DSN                 string
	DefaultUser         string `mapstructure:"default_user"`

	db *gorm.DB
}

var _ Database = (*Gorm)(nil)

func NewGorm(
	conf config.Database,
	destinations []config.Destination,
	apiKeys []config.APIKey,
) (*Gorm, error) {
	rc := Gorm{
		conf:                conf,
		destinations:        destinations,
		apiKeyToDestination: map[string]int64{},
		adminAPIKeys:        apiKeys,
	}

	for i, destination := range destinations {
		for _, apiKey := range destination.APIKeys {
			rc.apiKeyToDestination[apiKey] = int64(i)
		}
	}
	var (
		db  *gorm.DB
		err error
	)
	switch conf.Type {
	case "sqlite":
		db, err = gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	case "postgres":
		db, err = gorm.Open(postgres.Open(conf.DSN), &gorm.Config{})
	default:
		return nil, fmt.Errorf("unknown database type: %s", conf.Type)
	}
	if err != nil {
		return nil, err
	}

	rc.db = db

	err = db.AutoMigrate(
		&ShareLink{},
		&Team{},
		&User{},
		&Destination{},
		&APIKey{},
	)
	if err != nil {
		return nil, err
	}

	var teamCount int64
	db.Model(&Team{}).Count(&teamCount)
	if teamCount == 0 {
		team := Team{Name: rc.DefaultUser}
		db.Create(&team)

		destination := Destination{TeamID: team.ID, Name: "Local DuckDB", Type: "duckdb", Settings: `{"file": "data.duckdb"}`}
		db.Create(&destination)

		apiKey := APIKey{DestinationID: destination.ID, HashedAPIKey: rc.Hash("local")}
		db.Create(&apiKey)

		user := User{Teams: []*Team{&team}, Email: rc.DefaultUser, AuthType: "google"}
		db.Create(&user)
	}

	return &rc, nil
}

func (s *Gorm) VerifyAdminAPIKey(ctx context.Context, apiKey string) bool {
	for _, key := range s.adminAPIKeys {
		if key.Key == apiKey {
			return true
		}
	}
	return false
}

func (s *Gorm) CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error) {
	id := uuid.New()
	link := ShareLink{
		UUID:          id.String(),
		DestinationID: destId,
		Query:         query,
		ExpiresAt:     time.Now().Add(expires),
	}

	log.Print(link)
	log.Print(time.Now())

	res := s.db.Create(&link)
	if res.Error != nil {
		return uuid.Nil, res.Error
	}

	return id, nil
}

func (s *Gorm) GetShareQuery(ctx context.Context, queryId uuid.UUID) (SharedQuery, bool) {
	var link ShareLink
	res := s.db.First(&link, "uuid = ? AND expires_at > ?", queryId.String(), time.Now())
	if res.Error != nil {
		if !errors.Is(res.Error, gorm.ErrRecordNotFound) {
			log.Error().Err(res.Error).Str("query_id", queryId.String()).Msg("Unable to find shared query")
		}

		return SharedQuery{}, false
	}

	rc := SharedQuery{
		ID:            link.UUID,
		Query:         link.Query,
		ExpiresAt:     link.ExpiresAt,
		DestinationID: link.DestinationID,
	}

	return rc, true
}

func (s *Gorm) getTeamId(userId uint) uint {
	var user User

	s.db.Preload("Teams").First(&user, userId)
	if len(user.Teams) == 0 {
		return 0
	}

	return uint(user.Teams[0].ID)
}

// AddAPIKey implements database.ProprietaryDB.
func (*Gorm) AddAPIKey(ctx context.Context, destId int64, hashedAPIKey string) error {
	panic("unimplemented")
}

// CreateDestination implements database.ProprietaryDB.
func (s *Gorm) CreateDestination(ctx context.Context, userId uint, destType string, settings map[string]any) (config.Destination, error) {
	teamId := s.getTeamId(userId)
	if teamId == 0 {
		return config.Destination{}, errors.New("invalid team")
	}

	// dest := &Destination{
	// 	TeamID: teamId,
	// 	Type: destType,
	// 	Settings: settings,
	// }

	// res := db.db.Transaction(func(tx *gorm.DB) error {
	// 	result := tx.Where(User{Email: email, AuthType: source}).FirstOrCreate(&user)
	// 	if result.Error != nil {
	// 		return result.Error
	// 	}

	// 	if result.RowsAffected == 1 {
	// 		team := &Team{Name: email, Users: []*User{user}}
	// 		result = tx.Create(team)
	// 		if result.Error != nil {
	// 			return result.Error
	// 		}
	// 	}

	// 	return nil
	// })

	return config.Destination{}, errors.New("not implemented")
}

// GetDestinations implements database.ProprietaryDB.
func (s *Gorm) GetDestinations(c context.Context, userId uint) []config.Destination {
	var destinations []Destination
	teamId := s.getTeamId(userId)
	s.db.Where("team_id = ?", teamId).Find(&destinations)

	rc := make([]config.Destination, len(destinations))
	for i, dest := range destinations {
		rc[i].ID = int64(dest.ID)
		rc[i].Name = dest.Name

		err := json.Unmarshal([]byte(dest.Settings), &rc[i].Settings)
		if err != nil {
			log.Error().Err(err).Uint("team_id", teamId).Uint("destination_id", dest.ID).Msg("Unable to marshal settings json to map")
		}

		rc[i].Type = dest.Type
	}

	return rc
}

func (s *Gorm) Hash(str string) string {
	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:])
}

func (s *Gorm) GetUser(userId int64) *User {
	var user User
	tx := s.db.First(&user, userId)
	if tx.Error != nil {
		log.Error().Err(tx.Error).Msg("Unable to get user")
	}
	return &user
}

func (s *Gorm) CreateUser(email string, source string, details string) (*User, error) {
	user := &User{
		Email:       email,
		AuthType:    source,
		AuthDetails: details,
	}

	res := s.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Where(User{Email: email, AuthType: source}).FirstOrCreate(&user)
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 1 {
			team := &Team{Name: email, Users: []*User{user}}
			result = tx.Create(team)
			if result.Error != nil {
				return result.Error
			}
		}

		return nil
	})

	// result := db.db.Where(User{Email: email, AuthType: source}).FirstOrCreate(&user)
	return user, res
}

func (s *Gorm) GetAPIKeyDetails(ctx context.Context, apiKey string) (APIKey, error) {
	dbId, ok := s.apiKeyToDestination[apiKey]
	if !ok {
		return APIKey{}, errors.New("invalid API key")
	}
	rc := APIKey{
		DestinationID: uint(dbId),
	}

	// XXX breadchris from proprietary, is this needed?
	// var rc APIKey
	// var dbKey APIKey
	//
	// tx := s.db.First(&dbKey, "hashed_api_key = ?", k)
	// if tx.RowsAffected != 0 {
	// 	rc.DestinationID = int64(dbKey.DestinationID)
	// }

	return rc, nil
}

func (s *Gorm) GetDestinationCredentials(ctx context.Context, destinationId int64) (config.Destination, error) {
	var rc config.Destination
	var dbDestination Destination

	tx := s.db.First(&dbDestination, destinationId)

	if tx.RowsAffected != 0 {
		rc.Type = dbDestination.Type

		var result map[string]any
		err := json.Unmarshal([]byte(dbDestination.Settings), &result)
		if err != nil {
			return config.Destination{}, err
		}
		rc.Settings = result
	}

	return rc, tx.Error
}