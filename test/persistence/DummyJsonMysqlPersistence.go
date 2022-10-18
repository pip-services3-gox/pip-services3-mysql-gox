package test

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mysql-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-mysql-gox/test/fixtures"
)

type DummyJsonMySqlPersistence struct {
	*persist.IdentifiableJsonMySqlPersistence[fixtures.Dummy, string]
}

func NewDummyJsonMySqlPersistence() *DummyJsonMySqlPersistence {
	c := &DummyJsonMySqlPersistence{}
	c.IdentifiableJsonMySqlPersistence = persist.InheritIdentifiableJsonMySqlPersistence[fixtures.Dummy, string](c, "dummies_json")
	return c
}

func (c *DummyJsonMySqlPersistence) DefineSchema() {
	c.ClearSchema()
	c.EnsureTable("", "")
	c.EnsureSchema("ALTER TABLE `" + c.TableName + "` ADD `data_key` VARCHAR(50) AS (JSON_UNQUOTE(`data`->\"$.key\"))")
	c.EnsureIndex(c.TableName+"_json_key", map[string]string{"data_key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyJsonMySqlPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[fixtures.Dummy], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "data->'$.key'='" + key + "'"
	}

	return c.IdentifiableJsonMySqlPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		"", "",
	)
}

func (c *DummyJsonMySqlPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	filterObj := ""
	if key, ok := filter.GetAsNullableString("Key"); ok && key != "" {
		filterObj += "data->'$.key'='" + key + "'"
	}

	return c.IdentifiableJsonMySqlPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}

func (c *DummyJsonMySqlPersistence) GetOneRandom(ctx context.Context, correlationId string) (item fixtures.Dummy, err error) {
	return c.IdentifiableJsonMySqlPersistence.GetOneRandom(ctx, correlationId, "")
}
