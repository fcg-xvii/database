package sqlite

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestBase(t *testing.T) {
	path := "base.sqlite"
	log.Printf("Object path: %v\n", path)
	log.Println("Check database file created ...", CheckBaseFile(path))
	if CheckBaseFile(path) {
		os.Remove(path)
	}

	conn, err := Open(path)
	log.Println("Open dabase file", conn, err)
	if err != nil {
		t.Error(err)
	}

	cQuery := `
		create table goods (
			name text
		);`

	err = conn.Exec(cQuery)
	log.Println("Create goods table query ...", err)
	if err != nil {
		t.Error(err)
	}

	err = conn.Begin()
	log.Println("Start transaction ...", err)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("Good %v", i)
		rowid, err := conn.Insert("insert into goods values(?)", name)
		log.Println("Create good record ...", rowid, err)
		if err != nil {
			t.Error(err)
		}
	}

	err = conn.Commit()
	log.Println("Commit transaction ...", err)
	if err != nil {
		t.Error(err)
	}

	log.Println("..........................................")
	result := conn.Query("select rowid, * from goods")
	if result.Err != nil {
		t.Error(result.Err)
	}
	for result.Err == nil {
		log.Println(result.Row, result.Err)
		result.Next()
	}
	result.Close()

	log.Println("..........................................")

	row, err := conn.Row("select * from goods where rowid = 5")
	log.Println("Row select rowid = 5 ...", row, err, row.IsEmpty())

	err = conn.Close()
	log.Println("Close database ...", err)

	err = conn.Close()
	log.Println("Close closed database ...", err)
}
