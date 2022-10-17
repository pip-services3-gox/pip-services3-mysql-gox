package test

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mysql-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-mysql-gox/test/fixtures"
)

type DummyJsonMysqlPersistence struct {
	*persist.IdentifiableJsonMysqlPersistence[fixtures.Dummy, string]
}

func NewDummyJsonMysqlPersistence() *DummyJsonMysqlPersistence {
	c := &DummyJsonMysqlPersistence{}
	c.IdentifiableJsonMysqlPersistence = persist.InheritIdentifiableJsonMysqlPersistence[fixtures.Dummy, string](c, "dummies_json")
	return c
}

func (c *DummyJsonMysqlPersistence) DefineSchema() {
	c.ClearSchema()
	c.EnsureTable("", "")
	c.EnsureSchema("ALTER TABLE `" + c.TableName + "` ADD `data_key` VARCHAR(50) AS (JSON_UNQUOTE(`data`->\"$.key\"))")
	c.EnsureIndex(c.TableName+"_json_key", map[string]string{"data_key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyJsonMysqlPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[fixtures.Dummy], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "data->'$.key'='" + key + "'"
	}

	return c.IdentifiableJsonMysqlPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		"", "",
	)
}

func (c *DummyJsonMysqlPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	filterObj := ""
	if key, ok := filter.GetAsNullableString("Key"); ok && key != "" {
		filterObj += "data->'$.key'='" + key + "'"
	}

	return c.IdentifiableJsonMysqlPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}

func (c *DummyJsonMysqlPersistence) GetOneRandom(ctx context.Context, correlationId string) (item fixtures.Dummy, err error) {
	return c.IdentifiableJsonMysqlPersistence.GetOneRandom(ctx, correlationId, "")
}
