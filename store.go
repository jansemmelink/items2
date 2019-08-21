package items

import (
	"reflect"
)

//IStore of items
type IStore interface {
	Name() string
	Type() reflect.Type        //reflect.Type of registered IITem (ptr or struct)
	StructType() reflect.Type  //struct type = Type() or Type().Elem()
	Tmpl() IItem               //return a template item
	Add(IItem) (string, error) //add then return new id if added or error
	Get(string) (IItem, error) //get item with specified id
	Upd(string, IItem) error   //update item with specified id
	Del(id string) error       //delete item with specified id

	//Find returns a list of items, limited by size and applying the optional filter item
	//the map index is the item id that can be used to access the item
	//the items in the list are the list items with added _id field in JSON called ID in go
	Find(size int, filter IItem) []IDAndItem
}

//IDAndItem ...
type IDAndItem struct {
	ID   string
	Item IItem
}

//IItemWithID should NOT be supported by items,
//because an item should no know or care about its
//id, only the store knows the id.
//when creating a store, make sure this is not implemented
type IItemWithID interface {
	IItem
	ID() string
}
