package items

import (
	"reflect"
)

//IStore of items
type IStore interface {
	Name() string
	Type() reflect.Type
	Tmpl() IItem               //return a template item
	Add(IItem) (string, error) //add then return new id if added or error
	Get(string) (IItem, error) //get item with specified id
	Upd(string, IItem) error   //update item with specified id
	Del(id string) error       //delete item with specified id

	Find(size int, filter IItem) []IItem
}
