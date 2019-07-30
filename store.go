package items

import (
	"reflect"
)

//IStore of items
type IStore interface{
	//Name returns the item name, e.g. "group" or "person"
	Name() string

	ItemType() reflect.Type
	Get(id string) (IItem,error)
	Upd(IItem) error
	New(value interface{}) (IItem,error)
	Del(id string) error
	Find(size int/*filter...*/) []IItem
}
