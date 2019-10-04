//Package jsonfile implements a IItem store using a single JSON file with an array of items
package jsonfile

import (
	"encoding/json"
	"io"
	"os"
	"reflect"
	"regexp"
	"sync"

	items "github.com/jansemmelink/items2"
	"github.com/jansemmelink/log"
)

//MustNew calls New and panics on error
//parentDir where items must be stored in <dir>/<name>/<filename>.json
func MustNew(filename string, name string, tmpl items.IItem, idGen IIDGenerator) items.IStore {
	s, err := New(filename, name, tmpl, idGen)
	if err != nil {
		panic(log.Wrapf(err, "Failed to create jsonfile store"))
	}
	return s
}

var (
	validName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_]*[a-zA-Z0-9]$`)
)

//New makes a new items.IStore using a single JSON file
func New(filename string, name string, tmpl items.IItem, idGen IIDGenerator) (items.IStore, error) {
	if len(name) == 0 || !validName.MatchString(name) {
		return nil, log.Wrapf(nil, "New(name==%s) invalid identifier", name)
	}
	if tmpl == nil {
		return nil, log.Wrapf(nil, "New(tmpl==nil)")
	}
	if idGen == nil {
		return nil, log.Wrapf(nil, "New(idGen==nil)")
	}
	if _, ok := tmpl.(items.IItemWithID); ok {
		return nil, log.Wrapf(nil, "%T may not have ID() method.", tmpl)
	}
	s := &store{
		filename:      filename,
		itemName:      name,
		itemTmpl:      tmpl,
		itemType:      reflect.TypeOf(tmpl),
		fileItemType:  fileItemType(reflect.TypeOf(tmpl)),
		idGen:         idGen,
		itemsFromFile: make([]fileItem, 0),
		itemByID:      make(map[string]items.IItem),
	}

	if err := s.readFile(filename); err != nil {
		return nil, log.Wrapf(err, "cannot access items in JSON file %s", filename)
	}
	log.Debugf("Created JSON file store of %d %ss from file %s", len(s.itemByID), s.itemName, s.filename)
	return s, nil
} //New()

//store implements items.IStore for a directory with one JSON file per item
type store struct {
	mutex         sync.Mutex
	filename      string
	itemName      string
	itemTmpl      items.IItem
	itemType      reflect.Type
	fileItemType  reflect.Type
	idGen         IIDGenerator
	itemsFromFile []fileItem
	itemByID      map[string]items.IItem
}

//Name ...
func (s *store) Name() string {
	return s.itemName
}

//Type ...
func (s *store) Type() reflect.Type {
	return s.itemType
}

//StructType ...
func (s *store) StructType() reflect.Type {
	if s.itemType.Kind() == reflect.Ptr {
		return s.itemType.Elem()
	}
	return s.itemType
}

//Tmpl ...
func (s *store) Tmpl() items.IItem {
	return s.itemTmpl
}

func (s *store) Add(item items.IItem) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if item == nil {
		return "", log.Wrapf(nil, "cannot add nil item")
	}
	if err := item.Validate(); err != nil {
		return "", log.Wrapf(err, "cannot add invalid item")
	}
	//todo: add index functions, e.g. check for unique name in the store

	//assign a new unique id
	id := s.idGen.NewID()
	if _, ok := s.itemByID[id]; ok {
		return "", log.Wrapf(nil, "New %s.id=%s already exists", s.Name(), id)
	}

	//append and update file
	updatedItemsFromFile := append(s.itemsFromFile, fileItem{ID: id, Item: item})
	if err := s.updateFile(updatedItemsFromFile); err != nil {
		return "", log.Wrapf(err, "failed to update JSON file")
	}
	s.itemByID[id] = item
	log.Debugf("ADD(%s)", id)

	if addedItem, ok := item.(items.IItemWithNotifyNew); ok {
		addedItem.NotifyNew()
	}
	return id, nil
} //store.Add()

func (s *store) Upd(id string, item items.IItem) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if item == nil {
		return log.Wrapf(nil, "cannot upd nil item")
	}
	if err := item.Validate(); err != nil {
		return log.Wrapf(err, "cannot upd invalid item")
	}

	//replace and update file
	var oldItem items.IItem
	updatedItemsFromFile := append([]fileItem{}, s.itemsFromFile...)
	updIndex := -1
	for index, fileItem := range updatedItemsFromFile {
		if fileItem.ID == id {
			oldItem = updatedItemsFromFile[index].Item
			updatedItemsFromFile[index].Item = item
			updIndex = index
			break
		}
	}
	if updIndex < 0 {
		return log.Wrapf(nil, "id=%s does not exist", id)
	}

	if err := s.updateFile(updatedItemsFromFile); err != nil {
		return log.Wrapf(err, "failed to update JSON file")
	}

	s.itemsFromFile = updatedItemsFromFile
	s.itemByID[id] = item
	log.Debugf("UPD(%s) -> %+v", id, item)
	if updatedItem, ok := item.(items.IItemWithNotifyUpd); ok {
		updatedItem.NotifyUpd(oldItem)
	} else {
		log.Debugf("%s(%s).notifyUpd() not implemented by %T", s.itemName, id, item)
	}
	return nil
} //store.Upd()

func (s *store) Del(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var deletedItem items.IItemWithNotifyDel

	//make list of items without this one
	updatedItemsFromFile := make([]fileItem, 0)
	for _, fileItem := range s.itemsFromFile {
		if fileItem.ID != id {
			updatedItemsFromFile = append(updatedItemsFromFile, fileItem)
		} else {
			var ok bool
			deletedItem, ok = fileItem.Item.(items.IItemWithNotifyDel)
			if !ok {
				log.Debugf("%s(%s).notifyDel() not implemented by %T", s.itemName, id, fileItem.Item)
			} else {
				log.Debugf("%s(%s).notifyDel() is implemented by %T", s.itemName, id, fileItem.Item)
			}
		}
	}
	//update the file contents
	if err := s.updateFile(updatedItemsFromFile); err != nil {
		return log.Wrapf(err, "failed to update JSON file")
	}

	if deletedItem != nil {
		log.Debugf("NotifyDel(%s)", id)
		deletedItem.NotifyDel()
	} else {
		log.Debugf("Not calling NotifyDel(%s)", id)
	}
	//deleted: update store
	s.itemsFromFile = updatedItemsFromFile
	delete(s.itemByID, id)
	//not found also return success
	log.Debugf("DEL(%s)", id)
	return nil
} //store.Del()

func (s *store) Get(id string) (items.IItem, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	existing, ok := s.itemByID[id]
	if !ok {
		return nil, log.Wrapf(nil, "%s.id=%s does not exist", s.Name(), id)
	}
	return existing, nil
}

func (s *store) Find(size int, filter items.IItem) []items.IDAndItem {
	//do not lock, because we use Get() inside this func...
	//walk the items array to return in the order of the file
	list := make([]items.IDAndItem, 0)
	for _, fileItem := range s.itemsFromFile {
		if filter != nil {
			if err := fileItem.Item.Match(filter); err != nil {
				//log.Errorf("Filter out file %s: %+v", info.Name(), err)
				continue
			}
		}
		list = append(list, items.IDAndItem{ID: fileItem.ID, Item: fileItem.Item})
		if size > 0 && len(list) >= size {
			break
		}
	} //for each item from file
	return list
} //store.Find()

func (s *store) GetBy(key map[string]interface{}) (string, items.IItem, error) {
	//walk the items array to return first match
	for _, fileItem := range s.itemsFromFile {
		item := fileItem.Item
		if item.MatchKey(key) {
			return fileItem.ID, item, nil
		}
	} //for each item from file

	return "", nil, log.Wrapf(nil, "%s{%v} not found", s.itemName, key)
} //store.GetBy()

func (s *store) newItem() items.IItem {
	t := s.itemType
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	ni := reflect.New(t).Interface()
	return ni.(items.IItem)
}

func (s *store) noItem() items.IItem {
	ni := reflect.New(s.itemType).Interface()
	return ni.(items.IItem)
}

//IIDGenerator generates unique ids
type IIDGenerator interface {
	NewID() string
}

//read the file into the store, replacing old contents on success only
func (s *store) readFile(filename string) error {
	//if file does not exist, we can create the file later, but we need to ensure
	//we can access and create the file, so read/create it now...
	if _, err := os.Stat(filename); err != nil {
		f, err := os.Create(filename)
		if err != nil {
			return log.Wrapf(err, "cannot create file %s", filename)
		}

		//created empty file
		//store now has empty list
		f.Close()
		//s.itemsFromFile = make([]*IItemWithID, 0)
		s.itemByID = make(map[string]items.IItem)
		return nil
	}

	//filename exists
	f, err := os.Open(filename)
	if err != nil {
		return log.Wrapf(err, "cannot access file %s", filename)
	}
	defer f.Close()

	//make an array []IItem using the store's fileItemType (including _id)
	sliceType := reflect.SliceOf(s.fileItemType)
	itemSlicePtrValue := reflect.New(sliceType)
	itemSlicePtr := itemSlicePtrValue.Interface()

	//decode the file into the new slice:
	if err := json.NewDecoder(f).Decode(itemSlicePtr); err != nil {
		if err != io.EOF {
			return log.Wrapf(err, "failed to read file %s into %T", filename, itemSlicePtr)
		}
		//EOF: empty JSON file
		//store now has empty list
		//s.itemsFromFile = make([]*IItemWithID, 0)
		s.itemByID = make(map[string]items.IItem)
		return nil
	}

	//copy into array and id-map and ensure ids are unique
	itemsFromFile := make([]fileItem, 0)
	itemByID := make(map[string]items.IItem)
	needUpdate := false
	for i := 0; i < itemSlicePtrValue.Elem().Len(); i++ {
		fileItemValue := itemSlicePtrValue.Elem().Index(i)
		id := fileItemValue.Field(0).Interface().(string)
		itemData := fileItemValue.Field(1).Interface()

		//hack for reload of pre-_id-files
		if len(id) == 0 {
			id = s.idGen.NewID()
			needUpdate = true
		}

		item := itemData.(items.IItem)
		if len(id) == 0 {
			return log.Wrapf(nil, "Missing id in file %s %s[%d]", filename, s.Name(), i)
		}
		if _, ok := itemByID[id]; ok {
			return log.Wrapf(nil, "Duplicate id in file %s %s[%d].id=\"%s\"", filename, s.Name(), i, id)
		}

		if err := item.Validate(); err != nil {
			return log.Wrapf(err, "file %s %s[%d].id=%s is invalid", filename, s.Name(), i, id)
		}

		itemsFromFile = append(itemsFromFile, fileItem{ID: id, Item: item})
		itemByID[id] = item
		log.Debugf("LOADED %s[%d]: id=%s: %+v", filename, i, id, item)
	}

	if needUpdate {
		if err := s.updateFile(itemsFromFile); err != nil {
			return log.Wrapf(err, "Failed to update file %s with new ids", filename)
		}
	}

	//replace the old list
	s.itemsFromFile = itemsFromFile
	s.itemByID = itemByID

	//call NotifyNew for all loaded items
	log.Debugf("Calling notifyNew() for %d items loaded from file", len(s.itemsFromFile))
	for _, fileItem := range s.itemsFromFile {
		if newItem, ok := fileItem.Item.(items.IItemWithNotifyNew); ok {
			log.Debugf("%s(%s).notifyNew() ...", s.itemName, fileItem.ID)
			newItem.NotifyNew()
		} else {
			log.Debugf("%s(%s).notifyNew() not implemented by %T", s.itemName, fileItem.ID, fileItem.Item)
		}
	}
	return nil
} //store.readFile()

func (s *store) updateFile(updatedItems []fileItem) error {
	f, err := os.Create(s.filename)
	if err != nil {
		return log.Wrapf(err, "Failed to create new file %s", s.filename)
	}
	defer f.Close()

	jsonFileData, _ := json.MarshalIndent(updatedItems, "", "  ")
	_, err = f.Write(jsonFileData)
	if err != nil {
		return log.Wrapf(err, "Failed to write updated items to file %s", s.filename)
	}

	//written: update store now
	s.itemsFromFile = updatedItems
	return nil
} //store.updateFile()

func (s *store) Uses(fieldName string, itemStore items.IStore) error {
	return log.Wrapf(nil, "Not yet implemented")
}

//mockItem implements IItem but is not used in this module
type mockItem struct {
	Name string
}

func (mockItem) Validate() error { return nil }
func (mockItem) Match(items.IItem) error {
	return nil
}

//fileItem is a struct similar to what we store in files
//we use this to copy and modify its relfect description
//to store other IItem structs
type fileItem struct {
	ID   string      `json:"_id"`
	Item items.IItem `json:"item"`
}

//add _id to existing IItem struct type to store in file and list output
func fileItemType(itemType reflect.Type) reflect.Type {
	//return reflect.TypeOf(fileItem{})

	//make struct same as fileItem but using user item type instead of generic IItem
	//becuase user type is a struct while IItem is just an interface
	t := reflect.TypeOf(fileItem{})
	structFields := make([]reflect.StructField, 0)
	for i := 0; i < t.NumField(); i++ {
		structFields = append(structFields, t.Field(i))
	}
	//modify:
	structFields[1].Type = itemType
	return reflect.StructOf(structFields)
}
