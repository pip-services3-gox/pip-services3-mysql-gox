package fixtures

import (
	"context"
	"testing"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	"github.com/stretchr/testify/assert"
)

type DummyPersistenceFixture struct {
	dummy1      Dummy
	dummy2      Dummy
	persistence IDummyPersistence
}

func NewDummyPersistenceFixture(persistence IDummyPersistence) *DummyPersistenceFixture {
	c := DummyPersistenceFixture{}
	c.dummy1 = Dummy{Id: "", Key: "Key 11", Content: "Content 1"}
	c.dummy2 = Dummy{Id: "", Key: "Key 2", Content: "Content 2"}
	c.persistence = persistence
	return &c
}

func (c *DummyPersistenceFixture) TestCrudOperations(t *testing.T) {
	var dummy1 Dummy
	var dummy2 Dummy

	result, err := c.persistence.Create(context.Background(), "", c.dummy1)
	assert.Nil(t, err)

	dummy1 = result
	assert.NotEqual(t, Dummy{}, dummy1)
	assert.NotNil(t, dummy1.Id)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	// Create another dummy by send pointer
	result, err = c.persistence.Create(context.Background(), "", c.dummy2)
	assert.Nil(t, err)

	dummy2 = result
	assert.NotEqual(t, Dummy{}, dummy2)
	assert.NotNil(t, dummy2.Id)
	assert.Equal(t, c.dummy2.Key, dummy2.Key)
	assert.Equal(t, c.dummy2.Content, dummy2.Content)

	page, err := c.persistence.GetPageByFilter(context.Background(), "", *cdata.NewEmptyFilterParams(), *cdata.NewPagingParams(0, 5, true))
	assert.Nil(t, err)

	assert.True(t, page.HasData())
	assert.Len(t, page.Data, 2)
	assert.True(t, page.HasTotal())
	assert.Equal(t, page.Total, 2)

	assert.True(t, page.Data[0].Key == dummy1.Key || page.Data[0].Key == dummy2.Key)
	assert.True(t, page.Data[1].Key == dummy1.Key || page.Data[1].Key == dummy2.Key)

	page, err = c.persistence.GetPageByFilter(context.Background(), "",
		*cdata.NewFilterParamsFromTuples("Key", "Key 11"),
		*cdata.NewPagingParams(0, 5, true),
	)
	assert.Nil(t, err)

	assert.True(t, page.HasData())
	assert.Len(t, page.Data, 1)
	assert.True(t, page.HasTotal())
	assert.Equal(t, page.Total, 1)

	assert.Equal(t, page.Data[0].Key, dummy1.Key)

	// Update the dummy
	dummy1.Content = "Updated Content 1"
	result, err = c.persistence.Update(context.Background(), "", dummy1)
	assert.Nil(t, err)

	assert.NotEqual(t, Dummy{}, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, dummy1.Content, result.Content)

	// Set the dummy (updating)
	dummy1.Content = "Updated Content 2"
	result, err = c.persistence.Set(context.Background(), "", dummy1)
	assert.Nil(t, err)

	assert.NotEqual(t, Dummy{}, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, dummy1.Content, result.Content)

	// Set the dummy (creating)
	dummy2.Id = "New_id"
	dummy2.Key = "New_key"
	result, err = c.persistence.Set(context.Background(), "", dummy2)
	assert.Nil(t, err)

	assert.NotEqual(t, Dummy{}, result)
	assert.Equal(t, dummy2.Id, result.Id)
	assert.Equal(t, dummy2.Key, result.Key)
	assert.Equal(t, dummy2.Content, result.Content)

	// Partially update the dummy
	updateMap := cdata.NewAnyValueMapFromTuples("content", "Partially Updated Content 1")
	result, err = c.persistence.UpdatePartially(context.Background(), "", dummy1.Id, *updateMap)
	assert.Nil(t, err)

	assert.NotEqual(t, Dummy{}, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, "Partially Updated Content 1", result.Content)

	// Get the dummy by Id
	result, err = c.persistence.GetOneById(context.Background(), "", dummy1.Id)
	assert.Nil(t, err)

	assert.NotEqual(t, Dummy{}, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, "Partially Updated Content 1", result.Content)

	// Delete the dummy
	result, err = c.persistence.DeleteById(context.Background(), "", dummy1.Id)
	assert.Nil(t, err)

	assert.NotEqual(t, Dummy{}, result)
	assert.Equal(t, dummy1.Id, result.Id)
	assert.Equal(t, dummy1.Key, result.Key)
	assert.Equal(t, "Partially Updated Content 1", result.Content)

	// Get the deleted dummy
	result, err = c.persistence.GetOneById(context.Background(), "", dummy1.Id)
	assert.Nil(t, err)
	assert.Equal(t, Dummy{}, result)
}

func (c *DummyPersistenceFixture) TestBatchOperations(t *testing.T) {
	var dummy1 Dummy
	var dummy2 Dummy

	// Create one dummy
	result, err := c.persistence.Create(context.Background(), "", c.dummy1)
	assert.Nil(t, err)

	dummy1 = result
	assert.NotEqual(t, Dummy{}, dummy1)
	assert.NotNil(t, dummy1.Id)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	// Create another dummy
	result, err = c.persistence.Create(context.Background(), "", c.dummy2)
	assert.Nil(t, err)

	dummy2 = result
	assert.NotEqual(t, Dummy{}, dummy2)
	assert.NotNil(t, dummy2.Id)
	assert.Equal(t, c.dummy2.Key, dummy2.Key)
	assert.Equal(t, c.dummy2.Content, dummy2.Content)

	// Read batch
	items, err := c.persistence.GetListByIds(context.Background(), "", []string{dummy1.Id, dummy2.Id})
	assert.Nil(t, err)

	assert.NotNil(t, items)
	assert.Len(t, items, 2)

	// Delete batch
	err = c.persistence.DeleteByIds(context.Background(), "", []string{dummy1.Id, dummy2.Id})
	assert.Nil(t, err)

	// Read empty batch
	items, err = c.persistence.GetListByIds(context.Background(), "", []string{dummy1.Id, dummy2.Id})
	assert.Nil(t, err)

	assert.Len(t, items, 0)
}

func (c *DummyPersistenceFixture) TestRandomOperation(t *testing.T) {
	var dummy1 Dummy
	var dummy2 Dummy

	result, err := c.persistence.GetOneRandom(context.Background(), "")
	assert.Nil(t, err)
	assert.Equal(t, Dummy{}, result)
	assert.Equal(t, result.Id, "")
	assert.Equal(t, result.Key, "")
	assert.Equal(t, result.Content, "")

	// Create one dummy
	result, err = c.persistence.Create(context.Background(), "", c.dummy1)
	assert.Nil(t, err)

	dummy1 = result
	assert.NotEqual(t, Dummy{}, dummy1)
	assert.Equal(t, c.dummy1.Key, dummy1.Key)
	assert.Equal(t, c.dummy1.Content, dummy1.Content)

	// Create another dummy
	result, err = c.persistence.Create(context.Background(), "", c.dummy2)
	assert.Nil(t, err)

	dummy2 = result
	assert.NotEqual(t, Dummy{}, dummy2)
	assert.Equal(t, c.dummy2.Key, dummy2.Key)
	assert.Equal(t, c.dummy2.Content, dummy2.Content)

	result, err = c.persistence.GetOneRandom(context.Background(), "")
	assert.Nil(t, err)
	assert.NotEqual(t, Dummy{}, result)
}
