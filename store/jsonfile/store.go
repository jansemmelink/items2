//Package jsonfile implements a IItem store using a single JSON file with an array of items
package jsonfile

import (
	"encoding/json"
	"io"
	"os"
	"reflect"
	"sync"

	items "github.com/jansemmelink/items2"
	"github.com/jansemmelink/log"
	"github.com/satori/uuid"
)

//MustNew calls New and panics on error
//parentDir where items must be stored in <dir>/<name>/<filename>.json
func MustNew(filename string, name string, tmpl IItemWithID) items.IStore {
	s, err := New(filename, name, tmpl)
	if err != nil {
		panic(log.Wrapf(err, "Failed to create jsonfile store"))
	}
	return s
}

//New makes a new items.IStore using a single JSON file
func New(filename string, name string, tmpl IItemWithID) (items.IStore, error) {
	s := &store{
		filename:      filename,
		itemName:      name,
		itemTmpl:      tmpl,
		itemType:      reflect.TypeOf(tmpl),
		itemsFromFile: make([]*IItemWithID, 0),
		itemByID:      make(map[string]*IItemWithID),
	}
	if s.itemType.Kind() == reflect.Ptr {
		//dereference the &myStruct{} type to just myStruct{}
		s.itemType = s.itemType.Elem()
	}

	if err := s.readFile(filename); err != nil {
		return nil, log.Wrapf(err, "cannot access items in JSON file %s", filename)
	}
	log.Debugf("Created JSON files store of %d %ss from file %s", len(s.itemsFromFile), s.itemName, s.filename)
	return s, nil
} //New()

//store implements items.IStore for a directory with one JSON file per item
type store struct {
	mutex    sync.Mutex
	filename string
	itemName string
	itemTmpl items.IItem
	itemType reflect.Type

	itemsFromFile []*IItemWithID
	itemByID      map[string]*IItemWithID
}

//Name ...
func (s *store) Name() string {
	return s.itemName
}

//Type ...
func (s *store) Type() reflect.Type {
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

	//assign a new ID
	id, err := s.NewID(item)
	if err != nil {
		return "", log.Wrapf(err, "cannot assign unique id to new %s", s.Name())
	}

	//make sure it does not exist
	if _, ok := s.itemByID[id]; ok {
		return "", log.Wrapf(err, "%s.id=%s already exists", s.Name(), id)
	}

	//append and update file
	itemWithID := item.(IItemWithID)
	updatedItemsFromFile := append(s.itemsFromFile, &itemWithID)
	if err := s.updateFile(updatedItemsFromFile); err != nil {
		return "", log.Wrapf(err, "failed to update JSON file")
	}
	s.itemByID[id] = s.itemsFromFile[len(s.itemsFromFile)-1]
	log.Debugf("ADD(%s)", id)
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
	if item.(IItemWithID).ID() != id {
		return log.Wrapf(nil, "cannot upd id=%s with %s.id=%s", id, s.Name(), item.(IItemWithID).ID())
	}

	//replace and update file
	updatedItemsFromFile := s.itemsFromFile
	updIndex := -1
	for index, oldItemPtr := range updatedItemsFromFile {
		if (*oldItemPtr).ID() == id {
			itemWithID := item.(IItemWithID)
			updatedItemsFromFile[index] = &itemWithID
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
	s.itemByID[id] = s.itemsFromFile[updIndex]
	log.Debugf("UPD(%s) -> %+v", id, *s.itemByID[id])
	return nil
} //store.Upd()

func (s *store) Del(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	//remove and update file
	for index, oldItem := range s.itemsFromFile {
		if (*oldItem).ID() == id {
			updatedItemsFromFile := append(s.itemsFromFile[0:index-1], s.itemsFromFile[index:]...)
			if err := s.updateFile(updatedItemsFromFile); err != nil {
				return log.Wrapf(err, "failed to update JSON file")
			}
			delete(s.itemByID, id)
			return nil
		}
	}

	//not found also return success
	return nil
} //store.Del()

func (s *store) Get(id string) (items.IItem, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	existing, ok := s.itemByID[id]
	if !ok {
		return nil, log.Wrapf(nil, "%s.id=%s does not exist", s.Name(), id)
	}
	return *existing, nil
}

func (s *store) Find(size int, filter items.IItem) map[string]items.IItem {
	//do not lock, because we use Get() inside this func...
	// s.mutex.Lock()
	// defer s.mutex.Unlock()

	//walk the items array
	list := make(map[string]items.IItem, 0)
	for _, item := range s.itemsFromFile {
		if filter != nil {
			if err := (*item).Match(filter); err != nil {
				//log.Errorf("Filter out file %s: %+v", info.Name(), err)
				continue
			}
		}
		list[(*item).ID()] = *item
		if size > 0 && len(list) >= size {
			break
		}
	} //for each item from file
	return list
}

func (s *store) newItem() items.IItem {
	ni := reflect.New(s.itemType).Interface()
	return ni.(items.IItem)
}

func (s *store) noItem() items.IItem {
	ni := reflect.New(s.itemType).Interface()
	return ni.(items.IItem)
}

//IItemWithID must be supported for items used in this module
//because item id is not stored in filename etc, but in the item
type IItemWithID interface {
	items.IItem
	ID() string
}

func (s *store) NewID(item items.IItem) (string, error) {
	if item == nil {
		return "", log.Wrapf(nil, "NewID(nil)")
	}
	if itemWithID, ok := item.(IItemWithID); ok {
		return itemWithID.ID(), nil
	}
	return uuid.NewV1().String(), nil
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
		s.itemsFromFile = make([]*IItemWithID, 0)
		s.itemByID = make(map[string]*IItemWithID)
		return nil
	}

	//filename exists
	f, err := os.Open(filename)
	if err != nil {
		return log.Wrapf(err, "cannot access file %s", filename)
	}
	defer f.Close()

	//make an array []*IItem using the store's item type
	sliceType := reflect.SliceOf(reflect.PtrTo(s.itemType))
	itemSlicePtrValue := reflect.New(sliceType)
	itemSlicePtr := itemSlicePtrValue.Interface()

	//decode the file into the new slice:
	if err := json.NewDecoder(f).Decode(itemSlicePtr); err != nil {
		if err != io.EOF {
			return log.Wrapf(err, "failed to read file %s into %T", filename, itemSlicePtr)
		}
		//EOF: empty JSON file
		//store now has empty list
		s.itemsFromFile = make([]*IItemWithID, 0)
		s.itemByID = make(map[string]*IItemWithID)
		return nil
	}

	//copy into array and id-map and ensure ids are unique
	itemsFromFile := make([]*IItemWithID, 0)
	itemByID := make(map[string]*IItemWithID)
	for i := 0; i < itemSlicePtrValue.Elem().Len(); i++ {
		v := itemSlicePtrValue.Elem().Index(i).Interface()
		itemPtr := v.(IItemWithID)
		id := (itemPtr).ID()
		if len(id) == 0 {
			return log.Wrapf(nil, "Missing id in file %s %s[%d]", filename, s.Name(), i)
		}
		if _, ok := itemByID[id]; ok {
			return log.Wrapf(nil, "Duplicate id in file %s %s[%d].id=\"%s\"", filename, s.Name(), i, id)
		}

		if err := itemPtr.Validate(); err != nil {
			return log.Wrapf(err, "file %s %s[%d].id=%s is invalid", filename, s.Name(), i, id)
		}

		itemWithID := itemPtr.(IItemWithID)
		itemsFromFile = append(itemsFromFile, &itemWithID)
		itemByID[id] = &itemWithID
		log.Debugf("LOADED %s[%d]: (%T).id=%s: %+v", filename, i, itemWithID, id, itemWithID)
	}

	//make id index
	for _, item := range s.itemsFromFile {
		itemByID[(*item).ID()] = item
	}

	//replace the old list
	s.itemsFromFile = itemsFromFile
	s.itemByID = itemByID
	return nil
} //store.readFile()

func (s *store) updateFile(updatedItems []*IItemWithID) error {
	f, err := os.Create(s.filename)
	if err != nil {
		return log.Wrapf(err, "Failed to create new file %s", s.filename)
	}
	defer f.Close()

	jsonFileData, _ := json.Marshal(updatedItems)
	_, err = f.Write(jsonFileData)
	if err != nil {
		return log.Wrapf(err, "Failed to write updated items to file %s", s.filename)
	}

	//written: update store now
	s.itemsFromFile = updatedItems
	return nil
} //store.updateFile()
