package test

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mysql-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-mysql-gox/test/fixtures"
)

type DummyMysqlPersistence struct {
	*persist.IdentifiableMysqlPersistence[fixtures.Dummy, string]
}

func NewDummyMysqlPersistence() *DummyMysqlPersistence {
	c := &DummyMysqlPersistence{}
	c.IdentifiableMysqlPersistence = persist.InheritIdentifiableMysqlPersistence[fixtures.Dummy, string](c, "dummies")
	return c
}

func (c *DummyMysqlPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiableMysqlPersistence.DefineSchema()
	// Row name must be in double quotes for properly case!!!
	c.EnsureSchema("CREATE TABLE `" + c.TableName + "` (id VARCHAR(32) PRIMARY KEY, `key` VARCHAR(50), `content` TEXT)")
	c.EnsureIndex(c.IdentifiableMysqlPersistence.TableName+"_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyMysqlPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[fixtures.Dummy], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	sorting := ""

	return c.IdentifiableMysqlPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		sorting, "",
	)
}

func (c *DummyMysqlPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	return c.IdentifiableMysqlPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}

func (c *DummyMysqlPersistence) GetOneRandom(ctx context.Context, correlationId string) (item fixtures.Dummy, err error) {
	return c.IdentifiableMysqlPersistence.GetOneRandom(ctx, correlationId, "")
}
