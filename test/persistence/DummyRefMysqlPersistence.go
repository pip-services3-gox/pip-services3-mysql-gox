package test

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-mysql-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-mysql-gox/test/fixtures"
)

type DummyRefMySqlPersistence struct {
	persist.IdentifiableMySqlPersistence[*fixtures.Dummy, string]
}

func NewDummyRefMySqlPersistence() *DummyRefMySqlPersistence {
	c := &DummyRefMySqlPersistence{}
	c.IdentifiableMySqlPersistence = *persist.InheritIdentifiableMySqlPersistence[*fixtures.Dummy, string](c, "dummies")
	return c
}

func (c *DummyRefMySqlPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[*fixtures.Dummy], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	sorting := ""

	return c.IdentifiableMySqlPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		sorting, "",
	)
}

func (c *DummyRefMySqlPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "`key`='" + key + "'"
	}
	return c.IdentifiableMySqlPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
