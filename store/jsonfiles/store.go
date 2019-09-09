//Package jsonfiles implements a IItem store using a directory with one JSON file per item
package jsonfiles

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"

	items "github.com/jansemmelink/items2"
	"github.com/jansemmelink/log"
	"github.com/satori/uuid"
)

//MustNew calls New and panics on error
//parentDir where items must be stored in <dir>/<name>/<filename>.json
func MustNew(parentDir string, name string, tmpl items.IItem) items.IStore {
	s, err := New(parentDir, name, tmpl)
	if err != nil {
		panic(log.Wrapf(err, "Failed to create jsonfiles store"))
	}
	return s
}

//New makes a new items.IStore using a directory of JSON files
func New(parentDir string, name string, tmpl items.IItem) (items.IStore, error) {
	path := parentDir + "/" + name
	if err := mkdir(path); err != nil {
		return nil, log.Wrapf(err, "Cannot create directory \"%s\" for jsonfiles", path)
	}

	//todo:
	// if len(name) == 0 || !validName.MatchString(name) {
	// 	return nil, log.Wrapf(nil, "New(name==%s) invalid identifier", name)
	// }
	if tmpl == nil {
		return nil, log.Wrapf(nil, "New(tmpl==nil)")
	}
	//todo:
	// if idGen == nil {
	// 	return nil, log.Wrapf(nil, "New(idGen==nil)")
	// }
	if _, ok := tmpl.(items.IItemWithID); ok {
		return nil, log.Wrapf(nil, "%T may not have ID() method.", tmpl)
	}

	s := &store{
		path:            path,
		itemName:        name,
		itemTmpl:        tmpl,
		itemType:        reflect.TypeOf(tmpl),
		filenamePattern: fmt.Sprintf(`%s_(.*)\.json`, name),
	}
	if s.itemType.Kind() == reflect.Ptr {
		s.itemType = s.itemType.Elem()
	}
	s.filenameRegex = regexp.MustCompile(s.filenamePattern)

	//see if has any files in the dir, then see what's the latest existing id
	// filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
	// 	items := s.filenameRegex.FindStringSubmatch(path)
	// 	if len(items) >= 2 {
	// 		id, _ := strconv.Atoi(items[1])
	// 		if id >= s.nextID {
	// 			s.nextID = id + 1
	// 		}
	// 	}
	// 	return nil
	// })

	log.Debugf("Created JSON files store of %s in dir %s", s.itemName, s.path)
	return s, nil
} //New()

//store implements items.IStore for a directory with one JSON file per item
type store struct {
	mutex           sync.Mutex
	path            string
	itemName        string
	itemTmpl        items.IItem
	itemType        reflect.Type
	filenamePattern string
	filenameRegex   *regexp.Regexp
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

	//assign a new ID
	id := uuid.NewV1().String()

	//make sure it does not exist
	fn := s.itemFilename(id)
	if _, err := os.Stat(fn); err == nil {
		return "", log.Wrapf(err, "%s.id=%s already exists", s.Name(), id)
	}

	jsonItem, err := json.Marshal(item)
	if err != nil {
		return "", log.Wrapf(err, "Failed to JSON encode item")
	}
	f, err := os.Create(fn)
	if err != nil {
		return "", log.Wrapf(err, "Failed to create item file %s", fn)
	}
	defer f.Close()

	_, err = f.Write(jsonItem)
	if err != nil {
		return "", log.Wrapf(err, "Failed to write item to file %s", fn)
	}
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

	fn := s.itemFilename(id)
	if _, err := os.Stat(fn); err != nil {
		return log.Wrapf(nil, "%s.id=%s does not exist", s.Name(), id)
	}

	//load old item - needed when calling NotifyUpd
	oldItem, err := s.Get(id)

	jsonItem, err := json.Marshal(item)
	if err != nil {
		return log.Wrapf(err, "failed to JSON encode item")
	}
	f, err := os.Create(fn)
	if err != nil {
		return log.Wrapf(err, "failed to create item file %s", fn)
	}
	defer f.Close()

	_, err = f.Write(jsonItem)
	if err != nil {
		return log.Wrapf(err, "failed to write item to file %s", fn)
	}
	log.Debugf("UPD(%s)", id)
	if updatedItem, ok := item.(items.IItemWithNotifyUpd); ok {
		updatedItem.NotifyUpd(oldItem)
	}
	return nil
} //store.Upd()

func (s *store) Del(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if item, err := s.Get(id); err == nil && item != nil {
		if deletedItem, ok := item.(items.IItemWithNotifyDel); ok {
			deletedItem.NotifyDel()
		}
	}

	fn := s.itemFilename(id)
	err := os.Remove(fn)
	if err != nil {
		return log.Wrapf(err, "Cannot delete %s file: %s", s.itemName, fn)
	}
	return nil
}

func (s *store) Get(id string) (items.IItem, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	fn := s.itemFilename(id)
	jsonFile, err := os.Open(fn)
	if err != nil {
		return s.noItem(), log.Wrapf(err, "Cannot open %s file: %s", s.itemName, fn)
	}
	defer jsonFile.Close()

	newItemValue := reflect.New(s.itemType)
	newItemDataPtr := newItemValue.Interface()
	if err := json.NewDecoder(jsonFile).Decode(newItemDataPtr); err != nil {
		return s.noItem(), log.Wrapf(err, "Failed to decode JSON file %s into %s", fn, s.itemName)
	}
	newItem := newItemDataPtr.(items.IItem)
	if err := newItem.Validate(); err != nil {
		return s.noItem(), log.Wrapf(err, "Invalid %s in JSON file %s", s.itemName, fn)
	}
	return newItem, nil
}

func (s *store) Find(size int, filter items.IItem) []items.IDAndItem {
	//do not lock, because we use Get() inside this func...
	// s.mutex.Lock()
	// defer s.mutex.Unlock()

	//walk the directory
	list := make([]items.IDAndItem, 0)
	filepath.Walk(
		s.path,
		func(path string, info os.FileInfo, err error) error {
			if info.Mode().IsRegular() {
				parts := s.filenameRegex.FindStringSubmatch(path)
				log.Debugf("Eval file \"%s\" with %d parts: %v", info.Name(), len(parts), parts)
				if len(parts) >= 2 {
					id := parts[1] //parts[0] = full name, parts[1] = sub string match

					item, err := s.Get(id)
					if err != nil {
						//log.Errorf("List ignores file %s: %+v", info.Name(), err)
					} else {
						if filter != nil {
							if err := item.Match(filter); err != nil {
								//log.Errorf("Filter out file %s: %+v", info.Name(), err)
								item = nil
							}
						}
						if item != nil {
							list = append(list, items.IDAndItem{ID: id, Item: item})
							if size > 0 && len(list) >= size {
								//stop processing
								return filepath.SkipDir
							}
						}
					}
				}
			} //if regular file
			return nil
		})
	return list
}

func (s *store) itemFilename(id string) string {
	return fmt.Sprintf("%s/%s_%s.json", s.path, s.itemName, id)
}

func (s *store) newItem() items.IItem {
	ni := reflect.New(s.itemType).Interface()
	return ni.(items.IItem)
}

func (s *store) noItem() items.IItem {
	ni := reflect.New(s.itemType).Interface()
	return ni.(items.IItem)
}

func (s *store) Uses(fieldName string, itemStore items.IStore) error {
	return log.Wrapf(nil, "Not yet implemented")
}

func mkdir(dir string) error {
	dir = strings.TrimSuffix(dir, "/")
	info, err := os.Stat(dir)
	if err == nil && info.IsDir() {
		//already exists
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return log.Wrapf(err, "Cannot create the %s (notExist=%s, err=%s)", dir, os.ErrNotExist, err)
	}
	//does not exist, need to create
	//make sure parent exists, then continue
	parent := path.Dir(dir)
	if err := mkdir(parent); err != nil {
		return log.Wrapf(err, "mkdir(parent=%s) failed", parent)
	}
	err = os.Mkdir(dir, 0770)
	if err != nil {
		return log.Wrapf(err, "os.Mkdir(%s) failed", dir)
	}
	return nil
}
