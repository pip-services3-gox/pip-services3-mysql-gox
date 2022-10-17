package build

import (
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	cbuild "github.com/pip-services3-gox/pip-services3-components-gox/build"
	conn "github.com/pip-services3-gox/pip-services3-mysql-gox/connect"
)

// DefaultMysqlFactory creates Mysql components by their descriptors.
//	see Factory
//	see MysqlConnection
type DefaultMysqlFactory struct {
	*cbuild.Factory
}

//	Create a new instance of the factory.
func NewDefaultMysqlFactory() *DefaultMysqlFactory {

	c := &DefaultMysqlFactory{}
	c.Factory = cbuild.NewFactory()

	mysqlConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "mysql", "*", "1.0")
	c.RegisterType(mysqlConnectionDescriptor, conn.NewMysqlConnection)

	return c
}
