package jsonfile_test

import (
	"fmt"
	"os"
	"testing"

	items "github.com/jansemmelink/items2"
	"github.com/jansemmelink/items2/store/jsonfile"
	"github.com/jansemmelink/log"
	"github.com/satori/uuid"
)

func Test1(t *testing.T) {
	s1, err := jsonfile.New("./share/users.json", "user", user{}, idGen{})
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
			return log.Wrapf(err, "Get(id=%s)->(%p,%v)", id, item, err)
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
