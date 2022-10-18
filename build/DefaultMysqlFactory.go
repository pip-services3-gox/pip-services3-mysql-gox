package build

import (
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	cbuild "github.com/pip-services3-gox/pip-services3-components-gox/build"
	conn "github.com/pip-services3-gox/pip-services3-mysql-gox/connect"
)

// DefaultMySqlFactory creates MySql components by their descriptors.
//	see Factory
//	see MySqlConnection
type DefaultMySqlFactory struct {
	*cbuild.Factory
}

//	Create a new instance of the factory.
func NewDefaultMySqlFactory() *DefaultMySqlFactory {

	c := &DefaultMySqlFactory{}
	c.Factory = cbuild.NewFactory()

	mysqlConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "mysql", "*", "1.0")
	c.RegisterType(mysqlConnectionDescriptor, conn.NewMySqlConnection)

	return c
}
