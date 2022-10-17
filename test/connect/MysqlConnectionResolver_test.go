package test_connect

import (
	"context"
	"testing"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	conn "github.com/pip-services3-gox/pip-services3-mysql-gox/connect"
	"github.com/stretchr/testify/assert"
)

func TestMysqlConnectionResolver(t *testing.T) {

	dbConfig := cconf.NewConfigParamsFromTuples(
		"connection.host", "localhost",
		"connection.port", 3306,
		"connection.database", "test",
		"connection.ssl", false,
		"credential.username", "mysql",
		"credential.password", "mysql",
	)

	resolver := conn.NewMysqlConnectionResolver()
	resolver.Configure(context.Background(), dbConfig)

	uri, err := resolver.Resolve(context.Background(), "")
	assert.Nil(t, err)

	assert.NotEmpty(t, uri)
	assert.Equal(t, "mysql:mysql@tcp(localhost:3306)/test?ssl=false", uri)
}
