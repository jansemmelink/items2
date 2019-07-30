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

	s := &store{
		path:            path,
		itemName:        name,
		itemTmpl:        tmpl,
		itemType:        reflect.TypeOf(tmpl),
		filenamePattern: fmt.Sprintf("%s_([a-z0-9-]+).json", name),
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

	log.Debugf("JSONFILE(%s,%s)", s.path, s.itemName)
	return s, nil
}

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

// func (s *store) New(value interface{}) (items.IItem, error) {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()
// 	log.Debugf("NEW:%+v", value)

// 	//new item must assing an id
// 	ni := s.newItem().SetID(fmt.Sprintf("%d", s.nextID))

// 	//update the new item with the specified value
// 	niu, err := ni.Set(value)
// 	if err != nil {
// 		return nil, log.Wrapf(err, "Invalid value for new item")
// 	}

// 	//write item to file
// 	fn := s.itemFilename(niu.ID())
// 	f, err := os.Create(fn)
// 	if err != nil {
// 		return nil, log.Wrapf(err, "Cannot create new item file %s", fn)
// 	}
// 	defer f.Close()

// 	fileValue := itemFile{}
// 	fileValue.ID = niu.ID()
// 	fileValue.Revs = []itemRev{
// 		{Rev: 1, Value: value},
// 	}
// 	jsonFileValue, err := json.Marshal(fileValue)
// 	if err != nil {
// 		return nil, log.Wrapf(err, "Failed to encode item value as JSON")
// 	}

// 	_, err = f.Write(jsonFileValue)
// 	if err != nil {
// 		f.Close()
// 		os.Remove(fn)
// 		return nil, log.Wrapf(err, "Failed to write item value to file %s", fn)
// 	}

// 	s.nextID++

// 	log.Debugf("NEW(%s): id=%s, rev=%d value=%+v", s.Name(), niu.ID(), niu.Rev(), niu.Value())
// 	return niu, nil
// }

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
	//todo: add index functions, e.g. check for unique name in the store

	//assign a new ID
	id := uuid.NewV1().String()
	jsonItem, err := json.Marshal(item)
	if err != nil {
		return "", log.Wrapf(err, "Failed to JSON encode item")
	}
	fn := s.itemFilename(id)
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
	return id, nil
}

func (s *store) Upd(id string, item items.IItem) error {
	return log.Wrapf(nil, "NYI")
}

func (s *store) Del(id string) error {
	return log.Wrapf(nil, "NYI")
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

//Upd writes next rev to existing file
// func (s *store) Upd(i items.IItem) error {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()

// 	fn := s.itemFilename(i.ID())
// 	jsonFile, err := os.Open(fn)
// 	if err != nil {
// 		return log.Wrapf(err, "Cannot open %s file: %s", s.itemName, fn)
// 	}
// 	defer jsonFile.Close()

// 	//read existing data
// 	fileValue := itemFile{}
// 	if err := json.NewDecoder(jsonFile).Decode(&fileValue); err != nil {
// 		return log.Wrapf(err, "Failed to decode JSON file %s into %s", fn, s.itemName)
// 	}
// 	jsonFile.Close()

// 	if fileValue.ID != i.ID() {
// 		return log.Wrapf(nil, "File %s does not have id=%s", fn, i.ID())
// 	}
// 	if len(fileValue.Revs) < 1 {
// 		return log.Wrapf(nil, "File %s has no item revisions", fn)
// 	}

// 	lastRev := fileValue.Revs[len(fileValue.Revs)-1]
// 	if i.Rev() != lastRev.Rev+1 {
// 		return log.Wrapf(nil, "File %s has rev=%d. Cannot upd with rev=%d", fn, lastRev.Rev, i.Rev())
// 	}

// 	fileValue.Revs = append(fileValue.Revs, itemRev{Rev: i.Rev(), Value: i.Value()})

// 	//write new value to file
// 	jsonFileValue, _ := json.Marshal(fileValue)
// 	jsonFile, err = os.Create(fn)
// 	if err != nil {
// 		return log.Wrapf(err, "Failed to re-open file for update")
// 	}
// 	_, err = jsonFile.Write(jsonFileValue)
// 	if err != nil {
// 		return log.Wrapf(err, "Failed to write to re-opened file for update")
// 	}

// 	jsonFile.Close()
// 	return nil
// }

// func (s *store) Del(id string) error {
// 	s.mutex.Lock()
// 	defer s.mutex.Unlock()

// 	fn := s.itemFilename(id)
// 	err := os.Remove(fn)
// 	if err != nil {
// 		return log.Wrapf(err, "Failed to delete %s", fn)
// 	}
// 	return nil
// }

func (s *store) Find(size int, filter items.IItem) []items.IItem {
	// s.mutex.Lock()
	// defer s.mutex.Unlock()

	//walk the directory
	list := make([]items.IItem, 0)
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
						log.Errorf("List ignores file %s: %+v", info.Name(), err)
					} else {
						log.Debugf("Check")
						if filter != nil {
							log.Debugf("Check")
							if err := item.Match(filter); err != nil {
								log.Debugf("Check")
								log.Errorf("Filter out file %s: %+v", info.Name(), err)
								item = nil
							}
							log.Debugf("Check")
						}
						log.Debugf("Check")
						if item != nil {
							log.Debugf("Check")
							list = append(list, item)
							if size > 0 && len(list) >= size {
								//stop processing
								return filepath.SkipDir
							}
						} else {
							log.Debugf("Check")
						}
					}
				}
			} //if regular file
			return nil
		})
	return list
}

func (s *store) ItemType() reflect.Type {
	return s.itemType
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
