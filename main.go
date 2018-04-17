package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"log"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

var (
	target, source chan [2]string
	done           chan bool
)

func main() {
	done = make(chan bool)
	source = make(chan [2]string)
	target = make(chan [2]string)

	srcURI := "root:@/test"
	tgtURI := "root:@tcp(localhost:3306)/test"
	srcTable := "xxx"
	tgtTable := "xxxx"
	step := 1

	go compareMD5()

	go getMD5(srcURI, srcTable, step, source)
	go getMD5(tgtURI, tgtTable, step, target)

	<-done
}

func getMD5(URI, table string, step int, c chan [2]string) {
	var (
		err      error
		colNames []string
		cnt      int
		start    = 1

		// FIXME:(everpcpc) get primary key from schema
		selectSQL = fmt.Sprintf(`select * from %s where id>=? and id<?`, table)
	)
	defer close(c)

	db, err := sql.Open("mysql", URI)
	if err != nil {
		log.Fatalf("open db failed on %s: %+v", URI, err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("start transaction failed: %+v", err)
	}
	defer tx.Rollback()
	for {
		// log.Printf("start at: %d, %+v", start, URI)
		rows, err := tx.Query(selectSQL, start, start+step)
		if err != nil {
			log.Fatalf("select failed at {%d}: %+v", start, err)
		}
		if len(colNames) == 0 {
			colNames, err = rows.Columns()
			if err != nil {
				panic(err)
			}
		}

		h := md5.New()
		cnt = 0
		for rows.Next() {
			cnt++
			cols := make([]interface{}, len(colNames))
			raws := make([][]byte, len(colNames))
			for i := range cols {
				cols[i] = &raws[i]
			}
			err = rows.Scan(cols...)
			if err != nil {
				log.Fatalf("scan failed: {%d} %+v", start, err)
			}
			for i := range cols {
				_, err = h.Write(raws[i])
				if err != nil {
					log.Fatalf("write hash failed: {%d} %+v", start, err)
				}
			}
		}
		c <- [2]string{strconv.Itoa(start), string(h.Sum(nil))}
		if cnt < step {
			// log.Printf("finished scan at: {%d}", start)
			return
		}
		start += step
	}
}

func compareMD5() {
	for {
		s, more := <-source
		t, _ := <-target
		if !more {
			log.Println("OK")
			done <- true
			return
		}

		// log.Printf("OK: data ok at id=%s:%s: source(%x) = target(%x)", s[0], t[0], s[1], t[1])

		if s[0] != t[0] {
			log.Fatalf("ERR: sequence mismatch: source(%s) != target(%s)", s[0], t[0])
		}
		if s[1] != t[1] {
			// TODO:(everpcpc) check detailed difference
			log.Fatalf("ERR: data mismatch at id=%s: source(%x) != target(%x)", s[0], s[1], t[1])
		}

		// log.Printf("OK: data ok at id=%s: source(%x) = target(%x)", s[0], s[1], t[1])

	}
}
