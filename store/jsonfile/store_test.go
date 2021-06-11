package jsonfile_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	items "github.com/jansemmelink/items2"
	"github.com/jansemmelink/items2/store/jsonfile"
	"github.com/satori/uuid"
	"github.com/stewelarend/logger"
)

var log = logger.New()

func Test1(t *testing.T) {
	filename := "./share/users.json"
	os.Remove(filename)
	s1, err := jsonfile.New(filename, "user", user{}, idGen{})
	if err != nil {
		t.Fatalf("Failed to create s1: %+v", err)
	}
	log.Debugf("Created s1: %s", s1.Name())

	//add a single item (no duplicate detection)
	id1, err := s1.Add(user{Rev: 1, Name: "A"})
	if err != nil {
		t.Fatalf("Failed to add user: %v", err)
	}

	//get by ID
	{
		i1, err := s1.Get(id1)
		if err != nil {
			t.Fatalf("Failed to get u1: %v", err)
		}
		u1, ok := i1.(user)
		if !ok || u1.Rev != 1 || u1.Name != "A" {
			t.Fatalf("Retrieved u1 wrong values: %T %+v", i1, u1)
		}
	} //scope
}

func Test2(t *testing.T) {
	storeFileName := "./share/test2.json"
	os.Remove(storeFileName)
	store, err := jsonfile.New(storeFileName, "user", user{}, idGen{})
	if err != nil {
		t.Fatalf("Failed to create store file %s: %+v", storeFileName, err)
	}
	log.Debugf("Created store file %s: %s", storeFileName, store.Name())

	//add 10 items
	//log.DebugOn()
	userIDs := make([]string, 0)
	for i := 0; i < 10; i++ {
		id, err := store.Add(user{Rev: i + 1, Name: fmt.Sprintf("%c", 'a'+i)})
		if err != nil {
			t.Fatalf("Failed to add user[%d]: %v", i, err)
		}
		userIDs = append(userIDs, id)
	}

	//delete first item
	{
		if err := store.Del(userIDs[0]); err != nil {
			t.Fatalf("Failed to delete 0=%s: %v", userIDs[0], err)
		}
		if err := checkIDs(store, userIDs[1:]); err != nil {
			t.Fatalf("Failed to check all: %v", err)
		}
	}
} //Test2

func checkIDs(store items.IStore, ids []string) error {
	for _, id := range ids {
		item, err := store.Get(id)
		if err != nil || item == nil {
			return logger.Wrapf(err, "Get(id=%s)->(%p,%v)", id, item, err)
		}
	}
	log.Debugf("Found all %d ids=%v", len(ids), ids)
	return nil
}

type user struct {
	Rev  int    `json:"rev"`
	Name string `json:"name"`
}

func (u user) Validate() error {
	if len(u.Name) == 0 {
		return logger.Wrapf(nil, "user.name not specified")
	}
	return nil
}

func (u user) Match(filter items.IItem) error {
	return nil
}

func (u user) MatchKey(key map[string]interface{}) bool {
	checked := false
	if name, ok := key["name"]; ok {
		if u.Name != name {
			return false
		}
		checked = true
	}
	if rev, ok := key["rev"].(int); ok {
		if u.Rev != rev {
			return false
		}
		checked = true
	}
	return checked
}

type idGen struct{}

func (idGen) NewID() string {
	return uuid.NewV1().String()
}

type userUniq struct {
	user
}

//adding this method to the user makes it detect duplicate names
func (uu userUniq) Keys() map[string]interface{} {
	return map[string]interface{}{
		"name": uu.user.Name,
	}
}

func TestUniqueKeys(t *testing.T) {
	filename := "./share/userUniq.json"
	os.Remove(filename)
	store, err := jsonfile.New(filename, "userUniq", userUniq{}, idGen{})
	if err != nil {
		t.Fatalf("Failed to create store: %+v", err)
	}
	log.Debugf("Created store: %s", store.Name())

	//add a single item
	_, err = store.Add(userUniq{user: user{Rev: 1, Name: "A"}})
	if err != nil {
		t.Fatalf("Failed to add user: %v", err)
	}

	//adding same item should fail on duplicate keys
	_, err = store.Add(userUniq{user: user{Rev: 1, Name: "A"}})
	if err == nil {
		t.Fatalf("Added duplicate without error")
	}
	log.Debugf("Good: Not added duplicate: %v", err)

	//adding another item that should succeed:
	_, err = store.Add(userUniq{user: user{Rev: 1, Name: "B"}})
	if err != nil {
		t.Fatalf("Failed to add user: %v", err)
	}

	//find A then update A to B should fail
	id, item, err := store.GetBy(map[string]interface{}{"name": "A"})
	if err != nil {
		t.Fatalf("Failed to get A by name: %v", err)
	}
	u := item.(userUniq)
	u.Name = "B"
	if err = store.Upd(id, u); err == nil {
		t.Fatalf("Updated A to B which should have failed on duplicate key")
	}
	log.Debugf("Good: Failed to update A to B which would have created dup key")

	//update A to C should succeed
	u.Name = "C"
	if err = store.Upd(id, u); err != nil {
		t.Fatalf("Updated A to C failed: %v", err)
	}
	log.Debugf("Good: Updated A to C")

	//update B to A should now work because A no longer exists as it is now called C
	id, item, err = store.GetBy(map[string]interface{}{"name": "B"})
	if err != nil {
		t.Fatalf("Failed to get B by name: %v", err)
	}
	u = item.(userUniq)
	u.Name = "A"
	if err = store.Upd(id, u); err != nil {
		t.Fatalf("Updated B to A failed: %v", err)
	}
	log.Debugf("Good: Updated B to A")

	//delete C
	id, item, err = store.GetBy(map[string]interface{}{"name": "C"})
	if err != nil {
		t.Fatalf("Failed to get C by name: %v", err)
	}
	if err := store.Del(id); err != nil {
		t.Fatalf("Failed to delete C: %v", err)
	}

	//now adding C should work cause it was deleted
	_, err = store.Add(userUniq{user: user{Rev: 1, Name: "C"}})
	if err != nil {
		t.Fatalf("Failed to add user: %v", err)
	}

	//there should be only two items in the store and in the name index
	//but not yet testing this...
}

func TestFileUpdate(t *testing.T) {
	log = log.WithLevel(logger.LevelDebug)
	//delete the file to start with blank db
	filename := "./share/users.json"
	loadfilename := "./share/load/users.json"
	errorFilename := strings.Replace(loadfilename, ".json", ".err", 1)

	os.Mkdir("./share/load", 0770)
	os.Remove(filename)
	s1, err := jsonfile.NewWithReload(filename, loadfilename, "user", user{}, idGen{})
	if err != nil {
		t.Fatalf("Failed to create s1: %+v", err)
	}
	log.Debugf("Created s1: %s", s1.Name())

	if list := s1.Find(100, nil); len(list) != 0 {
		t.Fatalf("Got %d instead of 0", len(list))
	}

	//update the load file - still no items
	updateFile(t, loadfilename, `[]`)
	if list := s1.Find(100, nil); len(list) != 0 {
		t.Fatalf("Got %d instead of 0", len(list))
	}

	//update the load file - with one item
	updateFile(t, loadfilename, `[{"_id":"f8f47a3e-3601-11ea-8045-f45c89a88a57","item": {"name": "A", "rev":1}}]`)
	time.Sleep(time.Second)
	if list := s1.Find(100, nil); len(list) != 1 {
		t.Fatalf("Got %d instead of 1", len(list))
	}
	if !sameFileContents(t, filename, loadfilename) {
		t.Fatalf("Different contents in file %s and %s", filename, loadfilename)
	}

	//update the load file - with invalid item
	updateFile(t, loadfilename, `[{"_id":"f8f47a3e-3601-11ea-8045-f45c89a88a57","item": {"name": "", "rev":2}}]`)
	time.Sleep(time.Second)
	if list := s1.Find(100, nil); len(list) != 1 { //still expect old item to exist
		t.Fatalf("Got %d instead of 1", len(list))
	} else {
		u1, ok := list[0].Item.(user)
		if !ok || u1.Rev != 1 || u1.Name != "A" {
			t.Fatalf("Invalid update broke u1:%+v", u1)
		}
	}
	if sameFileContents(t, filename, loadfilename) { //should now be different
		t.Fatalf("Same contents in file %s and %s after invalid update", filename, loadfilename)
	}
	//check error file - should indicate invalid name ""
	checkErrorfile(t, errorFilename, "user.name not specified")

	//corrent the mistake, updating the item with a new name and rev
	updateFile(t, loadfilename, `[{"_id":"f8f47a3e-3601-11ea-8045-f45c89a88a57","item": {"name": "B", "rev":3}}]`)
	time.Sleep(time.Second)
	if list := s1.Find(100, nil); len(list) != 1 { //still expect old item to exist
		t.Fatalf("Got %d instead of 1", len(list))
	} else {
		u1, ok := list[0].Item.(user)
		if !ok || u1.Rev != 3 || u1.Name != "B" {
			t.Fatalf("Update not applied to u1:%+v", u1)
		}
	}
	if !sameFileContents(t, filename, loadfilename) { //should now be same
		t.Fatalf("Different contents in file %s and %s after update", filename, loadfilename)
	}
	//check error file must be deleted
	if _, err := os.Stat(errorFilename); err == nil {
		t.Fatalf("Error file %s still exists", errorFilename)
	}
}

func updateFile(t *testing.T, filename string, json string) {
	f, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filename, err)
	}
	defer f.Close()

	if _, err := f.Write([]byte(json)); err != nil {
		t.Fatalf("Failed to write to file %s: %v", filename, err)
	}
	t.Logf("Updated %s", filename)
}

func sameFileContents(t *testing.T, f1, f2 string) bool {
	md5_1 := fileMD5(t, f1)
	md5_2 := fileMD5(t, f2)
	return md5_1 == md5_2
}

func fileMD5(t *testing.T, filename string) string {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filename, err)
		return ""
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatalf("Failed to copy file contents: %v", err)
		return ""
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func checkErrorfile(t *testing.T, filename string, textToFind string) {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open %s: %v", filename, err)
	}
	defer f.Close()

	x := bytes.NewBuffer(nil)
	io.Copy(x, f)
	errorString := ""
	for {
		line, err := x.ReadString(byte('\n'))
		if len(line) > 0 {
			//log.Debugf("READ: %v", string(line))
			errorString += string(line)
		}
		if err != nil {
			break
		}
	} //for

	if strings.Index(errorString, textToFind) < 0 {
		t.Fatalf("Error file %s does not say \"%s\"", filename, textToFind)
	}

	t.Logf("Error file %s indicates \"%s\"", filename, textToFind)
}
