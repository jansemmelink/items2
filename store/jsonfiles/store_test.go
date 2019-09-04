package jsonfiles_test

import (
	"testing"

	items "github.com/jansemmelink/items2"
	"github.com/jansemmelink/items2/store/jsonfiles"
)

func Test1(t *testing.T) {
	store, err := jsonfiles.New("./share/users", "user", user{})
	if err != nil {
		t.Fatalf("Failed to create store: %+v", err)
	}
	t.Logf("Created store: %s", store.Name())
}

type user struct {
	rev int
}

func (u user) Rev() int {
	return u.rev
}

func (u user) Validate() error {
	return nil
}

func (u user) Match(filter items.IItem) error {
	return nil
}
