package items

//IItem is one item
type IItem interface {
	Validate() error

	//compare known specified fields in the other item (same type as you)
	// and return nil if match, else error to say what does not match
	Match(filter IItem) error

	//do exact comparison with specified fields
	MatchKey(key map[string]interface{}) bool
}

//IItemWithUniqueKeys is optional interface to implement if item has unique keys
type IItemWithUniqueKeys interface {
	IItem
	Keys() map[string]interface{}
}

//IItemWithNotifyNew is optional interface to implement to be notified of new items
type IItemWithNotifyNew interface {
	IItem
	NotifyNew()
}

//IItemWithNotifyUpd is optional interface to implement to be notified of updated items
type IItemWithNotifyUpd interface {
	IItem
	NotifyUpd(old IItem)
}

//IItemWithNotifyDel is optional interface to implement to be notified of deleted items
type IItemWithNotifyDel interface {
	IItem
	NotifyDel()
}
