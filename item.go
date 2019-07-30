package items

//IItem is one item
type IItem interface {
	Validate() error

	//compare known specified fields in the other item (same type as you)
	// and return nil if match, else error to say what does not match
	Match(filter IItem) error
}
