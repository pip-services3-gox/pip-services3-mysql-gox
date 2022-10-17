package test

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mysql-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-mysql-gox/test/fixtures"
)

type DummyRefMysqlPersistence struct {
	persist.IdentifiableMysqlPersistence[*fixtures.Dummy, string]
}

func NewDummyRefMysqlPersistence() *DummyRefMysqlPersistence {
	c := &DummyRefMysqlPersistence{}
	c.IdentifiableMysqlPersistence = *persist.InheritIdentifiableMysqlPersistence[*fixtures.Dummy, string](c, "dummies")
	return c
}

func (c *DummyRefMysqlPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[*fixtures.Dummy], err error) {

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

func (c *DummyRefMysqlPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	return c.IdentifiableMysqlPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
