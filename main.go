package main

import (
	"database/sql"
	"flag"
	"fmt"
	"hash/crc32"
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

	srcURI := flag.String("sourceURI", "root:@/test", "uri for source table")
	tgtURI := flag.String("targetURI", "", "uri for target table, default same with sourceURI")
	srcTable := flag.String("sourceTable", "", "table to check on source")
	tgtTable := flag.String("targetTable", "", "table to check on target, default same with sourceTable")
	step := flag.Uint64("step", 1, "step for batch check")
	flag.Parse()

	if *srcURI == "" {
		log.Fatal("sourceURI required")
	}
	if *tgtURI == "" {
		tgtURI = srcURI
	}

	if *srcTable == "" {
		log.Fatal("sourceTable required")
	}
	if *tgtTable == "" {
		tgtTable = srcTable
	}

	go compare()

	go getMD5(*srcURI, *srcTable, *step, source)
	go getMD5(*tgtURI, *tgtTable, *step, target)

	<-done
}

func getMD5(URI, table string, step uint64, c chan [2]string) {
	var (
		err          error
		colNames     []string
		start, maxID uint64

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

	var name, engine, version, rowFormat string
	var rows uint64
	var ignored *sql.RawBytes
	values := make([]interface{}, 18)
	for i := range values {
		values[i] = &ignored
	}
	values[0] = &name
	values[1] = &engine
	values[2] = &version
	values[3] = &rowFormat
	values[4] = &rows
	err = tx.QueryRow(`show table status like '` + table + `'`).Scan(values...)
	switch {
	case err == sql.ErrNoRows:
		log.Fatalf("table %s not found", table)
	case err != nil:
		log.Fatalf("get table status failed: %+v", err)
	}

	c <- [2]string{"engine", engine}
	c <- [2]string{"version", version}
	c <- [2]string{"rowFormat", rowFormat}
	// NOTE: sometimes not accurate, use maxID instead
	// c <- [2]string{"rows", strconv.FormatUint(rows, 10)}

	err = tx.QueryRow(fmt.Sprintf(`select id from %s order by id desc limit 1`, table)).Scan(&maxID)
	switch {
	case err == sql.ErrNoRows:
		log.Fatalf("table %s seems empty", table)
	case err != nil:
		log.Fatalf("get max id failed: %+v", err)
	}
	c <- [2]string{"maxID", strconv.FormatUint(maxID, 10)}

	start = 1
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

		total := []byte{}
		for rows.Next() {
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
				total = append(total, raws[i]...)
			}
		}
		c <- [2]string{strconv.FormatUint(start, 10), strconv.FormatUint(uint64(crc32.ChecksumIEEE(total)), 10)}
		if start+step > maxID {
			// log.Printf("finished scan at: {%d}", start)
			return
		}
		start += step
	}
}

func compare() {
	for {
		s, more := <-source
		t, _ := <-target
		if !more {
			log.Println("OK")
			done <- true
			return
		}
		if s[0] != t[0] {
			log.Fatalf("ERR: sequence mismatch: source(%s) != target(%s)", s[0], t[0])
		}
		if s[1] != t[1] {
			// TODO:(everpcpc) check detailed difference
			if _, err := strconv.ParseUint(s[0], 10, 0); err == nil {
				log.Fatalf("ERR: data mismatch at id=%s: source(%+v) != target(%+v)", s[0], s[1], t[1])
			} else {
				log.Fatalf("ERR: status mismatch at %s: source(%+v) != target(%+v)", s[0], s[1], t[1])
			}
		}
		// log.Printf("OK: data ok at id=%s:%s: source(%+v) = target(%+v)", s[0], t[0], s[1], t[1])
	}
}
