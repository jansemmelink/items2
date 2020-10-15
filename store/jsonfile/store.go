//Package jsonfile implements a IItem store using a single JSON file with an array of items
package jsonfile

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
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

//NewWithReload is same as New() then WatchFile()
func NewWithReload(filename string, reloadfilename string, name string, tmpl items.IItem, idGen IIDGenerator) (items.IStore, error) {
	s, err := newStore(filename, name, tmpl, idGen)
	if err != nil {
		return nil, err
	}
	s.watchFile(reloadfilename)
	return s, nil
}

//New makes a new items.IStore using a single JSON file
func New(filename string, name string, tmpl items.IItem, idGen IIDGenerator) (items.IStore, error) {
	return newStore(filename, name, tmpl, idGen)
}

func newStore(filename string, name string, tmpl items.IItem, idGen IIDGenerator) (*store, error) {
	filename = path.Clean(filename)
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
		indexSet:      newIndexSet(name),
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
	indexSet      indexSet

	watcher *fsnotify.Watcher
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

	if err := s.indexSet.CheckUniqueness("", item); err != nil {
		return "", log.Wrapf(err, "cannot add duplicate")
	}

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
	s.indexSet.AddToIndex(id, item)

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

	if err := s.indexSet.CheckUniqueness(id, item); err != nil {
		return log.Wrapf(err, "upd will make a duplicate")
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
	s.indexSet.DelFromIndex(id, oldItem)

	s.itemsFromFile = updatedItemsFromFile
	s.itemByID[id] = item
	s.indexSet.AddToIndex(id, item)
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

	var deletedItem items.IItem

	//make list of items without this one
	updatedItemsFromFile := make([]fileItem, 0)
	for _, fileItem := range s.itemsFromFile {
		if fileItem.ID != id {
			updatedItemsFromFile = append(updatedItemsFromFile, fileItem)
		} else {
			deletedItem = fileItem.Item
		}
	}

	if deletedItem != nil {
		//update the file contents
		if err := s.updateFile(updatedItemsFromFile); err != nil {
			return log.Wrapf(err, "failed to update JSON file")
		}
		s.indexSet.DelFromIndex(id, deletedItem)
		if deletedItemWithNotify, ok := deletedItem.(items.IItemWithNotifyDel); ok {
			log.Debugf("NotifyDel(%s)", id)
			deletedItemWithNotify.NotifyDel()
		} else {
			log.Debugf("Not calling NotifyDel(%s)", id)
		}
		//deleted: update store
		s.itemsFromFile = updatedItemsFromFile
		delete(s.itemByID, id)
		//not found also return success
		log.Debugf("DEL(%s)", id)
	}
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
	log.Debugf("Find among %d %s items...", len(s.itemsFromFile), s.Name())
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
	log.Debugf("%s.GetBy(%+v)", s.Name(), key)
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

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

	//copy into array and id-map and build new set of indexes to ensure ids are unique
	//(still not updating the store)
	itemsFromFile := make([]fileItem, 0)
	itemByID := make(map[string]items.IItem)
	indexSet := newIndexSet(s.itemName)
	needUpdate := false
	for i := 0; i < itemSlicePtrValue.Elem().Len(); i++ {
		fileItemValue := itemSlicePtrValue.Elem().Index(i)
		id := fileItemValue.Field(0).Interface().(string)
		log.Debugf("add [%d] id=%s  (has %d)", i, id, len(itemByID))
		if len(id) == 0 {
			return log.Wrapf(nil, "Missing id in file %s %s[%d]", filename, s.Name(), i)
		}
		if _, ok := itemByID[id]; ok {
			return log.Wrapf(nil, "Duplicate id in file %s %s[%d].id=\"%s\"", filename, s.Name(), i, id)
		}

		if fileItemValue.Field(1).IsNil() {
			return log.Wrapf(nil, "item[%d].id=%s has no item data", i, id)
		}
		itemData := fileItemValue.Field(1).Interface()
		item := itemData.(items.IItem)
		if err := item.Validate(); err != nil {
			return log.Wrapf(err, "file %s %s[%d].id=%s is invalid", filename, s.Name(), i, id)
		}

		//add to new index, list and map:
		if err := indexSet.AddToIndex(id, item); err != nil {
			return log.Wrapf(err, "file %s %s.id=%s has duplicate key", filename, s.Name(), id)
		}
		itemsFromFile = append(itemsFromFile, fileItem{ID: id, Item: item})
		itemByID[id] = item
		log.Debugf("LOADED %s[%d]: id=%s: %+v", filename, i, id, item)
	}

	//notify the application about upd/del changes first
	for id, oldItem := range s.itemByID {
		//see if exists in new file
		if newItem, ok := itemByID[id]; ok {
			if updatedItemWithNotify, ok := newItem.(items.IItemWithNotifyUpd); ok {
				log.Debugf("NotifyUpd(%s)", id)
				updatedItemWithNotify.NotifyUpd(oldItem)
			} else {
				log.Debugf("Not calling NotifyUpd(%s)", id)
			}
		} else {
			if deletedItemWithNotify, ok := oldItem.(items.IItemWithNotifyDel); ok {
				log.Debugf("NotifyDel(%s)", id)
				deletedItemWithNotify.NotifyDel()
			} else {
				log.Debugf("Not calling NotifyDel(%s)", id)
			}
		}
	}

	//notify the application about new items next
	for id, newItem := range itemByID {
		if _, ok := s.itemByID[id]; !ok {
			if addedItemWithNotify, ok := newItem.(items.IItemWithNotifyNew); ok {
				log.Debugf("NotifyNew(%s)", id)
				addedItemWithNotify.NotifyNew()
			} else {
				log.Debugf("Not calling NotifyNew(%s)", id)
			}
		}
	}

	if needUpdate {
		if err := s.updateFile(itemsFromFile); err != nil {
			return log.Wrapf(err, "Failed to update file %s with new ids", filename)
		}
	}

	//replace the old list, map and indexSet
	s.itemsFromFile = itemsFromFile
	s.itemByID = itemByID
	s.indexSet = indexSet
	return nil
} //store.readFile()

func (s *store) watchFile(filename string) error {
	//not using fsnotify.NewWatcher() anymore, because we only watch one file,
	//and want to trigger after the file changes stopped, not when it starts
	//so doing io.Stat() instead at regular intervals

	processModifiedFile := func(filename string) {
		log.Infof("Processing: %s", filename)
		errorFilename := strings.Replace(filename, ".json", ".err", 1)
		err := s.readFile(filename)
		if err != nil {
			log.Errorf("Reload failed: %v", err)

			//write error file
			if f, ferr := os.Create(errorFilename); ferr == nil {
				defer f.Close()
				f.Write([]byte(fmt.Sprintf("Reload failed: %+v", err)))
				log.Debugf("Wrote %s", errorFilename)
			} else {
				log.Errorf("Failed to create %s: %+v", errorFilename, ferr)
			}
		} else {
			log.Errorf("Reloaded %s", filename)
			os.Remove(errorFilename)

			//copy file to replace store file
			err := func(to, from string) error {
				f1, err := os.Open(from)
				if err != nil {
					return log.Wrapf(err, "Failed to open %s", from)
				}
				defer f1.Close()
				f2, err := os.Create(to)
				if err != nil {
					return log.Wrapf(err, "Failed to create %s", to)
				}
				defer f2.Close()
				if _, err := io.Copy(f2, f1); err != nil {
					return log.Wrapf(err, "Failed to copy %s to %s", from, to)
				}
				log.Debugf("Copied %s to %s", from, to)
				return nil
			}(s.filename, filename)
			if err != nil {
				log.Errorf("Failed to copy loaded file %s into store file %s: %v", filename, s.filename, err)
			}
		}
	} //processModifiedFile()

	go func(filename string) {
		lastModTime := time.Now()
		changing := false
		for {
			if info, err := os.Stat(filename); err == nil {
				if info.ModTime().After(lastModTime) {
					//detect a change
					changing = true
					lastModTime = info.ModTime()
				}
			} //if got file info

			//detect changes stopped for 3 seconds
			if changing && time.Now().After(lastModTime.Add(time.Second*3)) {
				log.Debugf("RELOADING %s ...", filename)
				processModifiedFile(filename)
				changing = false
			} else {
				if changing {
					log.Tracef("CHANGING  %s ...", filename)
				} else {
					log.Tracef("NO CHANGE %s ...", filename)
				}
			}

			//wait before checking again...
			time.Sleep(time.Second)
		} //forever...
	}(path.Clean(filename))

	log.Debugf("Watching %s...", filename)
	return nil
} //store.watchFile()

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

func newIndexSet(name string) indexSet {
	return indexSet{
		name:  name,
		index: make(map[string]itemIndex),
	}
}

type indexSet struct {
	name  string
	index map[string]itemIndex
}

func (s *indexSet) CheckUniqueness(id string, i items.IItem) error {
	//if i.id is defined, do not compare with self (e.g. during item update)
	//if i.id is not defined, its a new item that must be checked against all items
	if itemWithUniqueKeys, ok := i.(items.IItemWithUniqueKeys); ok {
		//need to check each unique key agains all other items
		log.Debugf("Checking unique keys on %T", i)
		keys := itemWithUniqueKeys.Keys()
		for n, v := range keys {
			//only need to check if this index exist
			//if not - it will be created when this item is added,
			//but then since its empty now, no need to check it if
			//it does not exist :-)
			if index, ok := s.index[n]; ok {
				if otherItemID, ok := index[v]; ok {
					//the index entry already exists:
					//if this item has no id, this is a new item and it will
					//be duplicate key
					if len(id) == 0 {
						return log.Wrapf(nil, "duplicate key: %s:{%s:%v}", s.name, n, v)
					}

					//item has an id, so we're busy updating an existing item:
					//this is only duplicate if the indexed item is not this item
					if otherItemID != id {
						return log.Wrapf(nil, "duplicate key: %s:{%s:%v} same as %s:{id:%s}", s.name, n, v, s.name, otherItemID)
					}
				}
			}
		}
	} else {
		log.Debugf("No unique keys on %T", i)
	}
	return nil
}

func (s *indexSet) AddToIndex(id string, i items.IItem) error {
	if itemWithUniqueKeys, ok := i.(items.IItemWithUniqueKeys); ok {
		keys := itemWithUniqueKeys.Keys()

		//check before adding
		for n, v := range keys {
			//create index if not exist
			index, ok := s.index[n]
			if !ok {
				index = newIndex()
				s.index[n] = index
			}
			if existingID, ok := index[v]; ok {
				if existingID != id {
					return log.Wrapf(nil, "%s.id=%s duplicate on %s=%v",
						s.name, id, n, v)
				}
			}
		} //for each item.key

		//no duplicates: add all keys
		for n, v := range keys {
			index, _ := s.index[n]
			index[v] = id
			log.Debugf("Added index(%s)[%v]=item", n, v)
		} //for each item.key
	}
	return nil
} //store.AddToIndex()

func (s *indexSet) DelFromIndex(id string, i items.IItem) {
	if itemWithUniqueKeys, ok := i.(items.IItemWithUniqueKeys); ok {
		keys := itemWithUniqueKeys.Keys()
		for n, v := range keys {
			//delete only if index exists
			index, ok := s.index[n]
			if ok {
				delete(index, v)
				log.Debugf("Removed index(%s)[%v]=item.id=%s", n, v, id)
			}
		}
	}
} //store.DelFromIndex()

//index stores the id
//use that to get the item in the store from itemByID[<id>]
type itemIndex map[interface{}]string

func newIndex() itemIndex {
	return make(map[interface{}]string)
}
