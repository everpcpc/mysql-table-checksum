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

type config struct {
	srcDB *sql.DB
	tgtDB *sql.DB
}

func main() {
	done = make(chan bool)
	source = make(chan [2]string, 5)
	target = make(chan [2]string, 5)

	srcURI := "root:@/test"
	tgtURI := "root:@/test"
	srcTable := "xxx"
	tgtTable := "xxxx"
	step := 1

	srcDB, err := sql.Open("mysql", srcURI)
	if err != nil {
		log.Fatalf("open db failed on %s: %+v", srcURI, err)
	}
	tgtDB, err := sql.Open("mysql", srcURI)
	if err != nil {
		log.Fatalf("open db failed on %s: %+v", tgtURI, err)
	}
	go compareMD5()

	go getMD5(srcDB, srcTable, step, source)
	go getMD5(tgtDB, tgtTable, step, target)

	<-done
}

func getMD5(db *sql.DB, table string, step int, c chan [2]string) {
	var (
		err      error
		start    = 1
		colNames []string
	)
	defer close(c)

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("start transaction failed: %+v", err)
	}
	defer tx.Rollback()
	sql := fmt.Sprintf(`select * from %s where id>=? and id<?`, table)
	for {
		log.Printf("start at: %d", start)
		rows, err := tx.Query(sql, start, start+step)
		if err != nil {
			log.Fatalf("select failed: {%s} %+v", sql, err)
		}
		if len(colNames) == 0 {
			colNames, err = rows.Columns()
			if err != nil {
				panic(err)
			}
		}

		h := md5.New()
		cnt := 0
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
			log.Printf("finished scan at: {%d}", start)
			return
		}
		start += step
	}
}

func compareMD5() {
	for {
		s, more := <-source
		t, _ := <-target
		if s[0] != t[0] {
			log.Fatalf("sequence mismatch: source(%s) != target(%s)", s[0], t[0])
		}
		if s[1] != t[1] {
			// TODO:(everpcpc) check detailed difference
			log.Fatalf("data mismatch at id=%s: source(%x) != target(%x)", s[0], s[1], t[1])
		}
		if !more {
			log.Println("all done.")
			done <- true
			return
		}
	}
}
