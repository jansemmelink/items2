package items

//IItem is one item
type IItem interface {
	Validate() error
	ID() string
	Rev() int
	Value() interface{}

	//SetID is called immediately after creation to set the ID
	//Changing the ID sets rev back to 1
	//If ID was not defined, rev is set to 0 so that first call to Set()
	//increments it to 1
	SetID(newID string) IItem

	//after loading from file, create new item and call this
	SetAll(store IStore, newID string, rev int, value interface{}) (IItem,error)

	//Set updates the value and validate it, then increments the revision
	Set(newValue interface{}) (IItem,error)
}
