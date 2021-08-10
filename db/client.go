package db

import (
	"context"
	"go_postgres/config"

	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
)

type dbClient struct {
	connConfig pgx.ConnConfig
}

func NewDbClient(DBconf config.Config) dbClient {
	conf, err := pgx.ParseConfig(DBconf.DBConnectionString)
	if err != nil {
		log.WithError(err).Fatal("Failed to connect PostgreSQL database")
	}
	//conf.PreferSimpleProtocol = true
	conf.RuntimeParams = map[string]string{
		"standard_conforming_strings": "on",
	}

	pingConn, err := pgx.Connect(context.Background(), DBconf.DBConnectionString)
	if err != nil {
		log.WithError(err).Fatal("Failed to connect PostgreSQL database")
	}
	err = pingConn.Ping(context.Background())
	if err != nil {
		log.WithError(err).Fatal("Database server is not available")
	}
	pingConn.Close(context.Background())
	return dbClient{connConfig: *conf}
}

func (o *dbClient) NewConnection() (*pgx.Conn, error) {
	return pgx.ConnectConfig(context.Background(), &o.connConfig)
}
