package test_connect

import (
	"context"
	"os"
	"testing"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	conn "github.com/pip-services3-gox/pip-services3-mysql-gox/connect"
	"github.com/stretchr/testify/assert"
)

func TestMysqlConnection(t *testing.T) {
	var connection *conn.MysqlConnection

	mysqlUri := os.Getenv("MYSQL_URI")
	mysqlHost := os.Getenv("MYSQL_HOST")
	if mysqlHost == "" {
		mysqlHost = "localhost"
	}
	mysqlPort := os.Getenv("MYSQL_PORT")
	if mysqlPort == "" {
		mysqlPort = "3306"
	}
	mysqlDatabase := os.Getenv("MYSQL_DB")
	if mysqlDatabase == "" {
		mysqlDatabase = "test"
	}
	mysqlUser := os.Getenv("MYSQL_USER")
	if mysqlUser == "" {
		mysqlUser = "mysql"
	}
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	if mysqlPassword == "" {
		mysqlPassword = "mysql"
	}

	if mysqlUri == "" && mysqlHost == "" {
		t.Skip("No MySql credentials")
	}

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.uri", mysqlUri,
		"connection.host", mysqlHost,
		"connection.port", mysqlPort,
		"connection.database", mysqlDatabase,
		"credential.username", mysqlUser,
		"credential.password", mysqlPassword,
		"options.max_pool_size", 10,
		"options.connect_timeout", 100,
		"options.idle_timeout", 100,
	)

	connection = conn.NewMysqlConnection()
	connection.Configure(context.Background(), dbConfig)
	err := connection.Open(context.Background(), "")
	assert.Nil(t, err)

	assert.NotNil(t, connection.GetConnection())
	assert.NotEmpty(t, connection.GetDatabaseName())
	assert.NotNil(t, connection.GetDatabaseName())

	err = connection.Close(context.Background(), "")
	assert.Nil(t, err)
}
