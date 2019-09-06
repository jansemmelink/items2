package items

//IItem is one item
type IItem interface {
	Validate() error

	//compare known specified fields in the other item (same type as you)
	// and return nil if match, else error to say what does not match
	Match(filter IItem) error
}

//IItemWithNotifyNew is optional interface to implement to be notified of new items
type IItemWithNotifyNew interface {
	IItem
	NotifyNew()
}

//IItemWithNotifyUpd is optional interface to implement to be notified of updated items
type IItemWithNotifyUpd interface {
	IItem
	NotifyUpd()
}

//IItemWithNotifyDel is optional interface to implement to be notified of deleted items
type IItemWithNotifyDel interface {
	IItem
	NotifyDel()
}
