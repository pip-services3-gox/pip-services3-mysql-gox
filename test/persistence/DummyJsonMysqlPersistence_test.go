package test

import (
	"context"
	"os"
	"testing"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	tf "github.com/pip-services3-gox/pip-services3-mysql-gox/test/fixtures"
)

func TestDummyJsonMySqlPersistence(t *testing.T) {

	var persistence *DummyJsonMySqlPersistence
	var fixture tf.DummyPersistenceFixture

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
		mysqlUser = "user"
	}
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	if mysqlPassword == "" {
		mysqlPassword = "password"
	}

	if mysqlUri == "" && mysqlHost == "" {
		t.Skip("Connection params not set")
	}

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.uri", mysqlUri,
		"connection.host", mysqlHost,
		"connection.port", mysqlPort,
		"connection.database", mysqlDatabase,
		"credential.username", mysqlUser,
		"credential.password", mysqlPassword,
	)

	persistence = NewDummyJsonMySqlPersistence()
	fixture = *tf.NewDummyPersistenceFixture(persistence)
	persistence.Configure(context.Background(), dbConfig)

	opnErr := persistence.Open(context.Background(), "")
	if opnErr != nil {
		t.Error("Error opened persistence", opnErr)
		return
	}

	defer func() {
		err := persistence.Close(context.Background(), "")
		if err != nil {
			panic(err)
		}
	}()

	opnErr = persistence.Clear(context.Background(), "")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("DummyMySqlConnection:CRUD", fixture.TestCrudOperations)

	opnErr = persistence.Clear(context.Background(), "")
	if opnErr != nil {
		t.Error("Error cleaned persistence", opnErr)
		return
	}

	t.Run("DummyMySqlConnection:Batch", fixture.TestBatchOperations)

}
