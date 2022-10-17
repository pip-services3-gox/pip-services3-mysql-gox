package test

import (
	"context"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mysql-gox/persistence"
)

type DummyMapMysqlPersistence struct {
	persist.IdentifiableMysqlPersistence[map[string]any, string]
}

func NewDummyMapMysqlPersistence() *DummyMapMysqlPersistence {
	c := &DummyMapMysqlPersistence{}
	c.IdentifiableMysqlPersistence = *persist.InheritIdentifiableMysqlPersistence[map[string]any, string](c, "dummies")
	return c
}

func (c *DummyMapMysqlPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiableMysqlPersistence.DefineSchema()
	c.EnsureSchema("CREATE TABLE `" + c.TableName + "` (id VARCHAR(32) PRIMARY KEY, `key` VARCHAR(50), `content` TEXT)")
	c.EnsureIndex(c.IdentifiableMysqlPersistence.TableName+"_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyMapMysqlPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[map[string]any], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	sorting := ""

	return c.IdentifiableMysqlPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging, sorting, "",
	)
}

func (c *DummyMapMysqlPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	return c.IdentifiableMysqlPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
