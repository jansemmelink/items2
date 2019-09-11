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
	store, err := jsonfile.New("./share/users.json", "user", user{}, idGen{})
	if err != nil {
		t.Fatalf("Failed to create store: %+v", err)
	}
	t.Logf("Created store: %s", store.Name())

	//add a single item (no duplicate detection)
	id1, err := store.Add(user{Rev: 1, Name: "A"})
	if err != nil {
		t.Fatalf("Failed to add user: %v", err)
	}

	//get by ID
	{
		i1, err := store.Get(id1)
		if err != nil {
			t.Fatalf("Failed to get u1: %v", err)
		}
		u1, ok := i1.(user)
		if !ok || u1.Rev != 1 || u1.Name != "A" {
			t.Fatalf("Retrieved u1 wrong values: %T %+v", i1, u1)
		}
	} //scope

	//use new store to ensure we don't load from memory
	s2, err := jsonfile.New("./share/users.json", "user", user{}, idGen{})
	if err != nil {
		t.Fatalf("Failed to create s2: %+v", err)
	}
	t.Logf("Created s2: %s", s2.Name())

	//get by ID
	{
		i1, err := s2.Get(id1)
		if err != nil {
			t.Fatalf("Failed to get u1 from s2: %v", err)
		}

		u1, ok := i1.(user)
		if !ok || u1.Rev != 1 || u1.Name != "A" {
			t.Fatalf("Retrieved u1 wrong values: %T %+v", i1, u1)
		}
	} //scope

	//delete from s2
	err = s2.Del(id1)
	if err != nil {
		t.Fatalf("Failed to delete u1 from s2: %v", err)
	}

	//make sure its not accessible in s1
	//get by ID
	{
		i1, err := store.Get(id1)
		if err == nil {
			t.Fatalf("Got u1 from s1 after deletion in s2")
		}
		if i1 != nil {
			t.Fatalf("Got u1 from s1 after deletion in s2")
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
	t.Logf("Created store file %s: %s", storeFileName, store.Name())

	//add 10 items
	log.DebugOn()
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
	return false
}

type idGen struct{}

func (idGen) NewID() string {
	return uuid.NewV1().String()
}
