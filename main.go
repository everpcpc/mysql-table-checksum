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
	step := flag.Uint64("step", 10, "step for batch check")
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
		err             error
		colNames        []string
		offset, maxID   uint64
		primaryKey      string
		primaryKeyIndex int
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
	var rowCount uint64
	var ignored *sql.RawBytes
	values := make([]interface{}, 18)
	for i := range values {
		values[i] = &ignored
	}
	values[0] = &name
	values[1] = &engine
	values[2] = &version
	values[3] = &rowFormat
	values[4] = &rowCount
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
	// c <- [2]string{"rowCount", strconv.FormatUint(rowCount, 10)}

	values = make([]interface{}, 13)
	for i := range values {
		values[i] = &ignored
	}
	values[4] = &primaryKey
	err = tx.QueryRow(fmt.Sprintf(`show index from %s where Key_name="PRIMARY"`, table)).Scan(values...)
	switch {
	case err == sql.ErrNoRows:
		log.Fatalf("table %s has no primary key", table)
	case err != nil:
		log.Fatalf("get primary key failed: %+v", err)
	}
	c <- [2]string{"primaryKey", primaryKey}
	// log.Printf("primary key for %s is %s", table, primaryKey)

	err = tx.QueryRow(fmt.Sprintf(`select %s from %s order by id desc limit 1`, primaryKey, table)).Scan(&maxID)
	switch {
	case err == sql.ErrNoRows:
		log.Fatalf("table %s seems empty", table)
	case err != nil:
		log.Fatalf("get max id failed: %+v", err)
	}
	c <- [2]string{"maxID", strconv.FormatUint(maxID, 10)}
	// log.Printf("max id for %s is %d", table, maxID)

	offset = 1
	selectSQL := fmt.Sprintf(`select * from %s where %s>=? limit ?`, table, primaryKey)
	for {
		// log.Printf("offset at: %d, %+v", offset, URI)
		rows, err := tx.Query(selectSQL, offset, step)
		if err != nil {
			log.Fatalf("select failed at {%d}: %+v", offset, err)
		}
		if len(colNames) == 0 {
			colNames, err = rows.Columns()
			if err != nil {
				log.Fatal(err)
			}
			for i := range colNames {
				if colNames[i] == primaryKey {
					primaryKeyIndex = i
				}
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
				log.Fatalf("scan failed: {%d} %+v", offset, err)
			}

			for i := range cols {
				total = append(total, raws[i]...)
				// append to split colunmns
				total = append(total, 0x00)
			}
			offset, err = strconv.ParseUint(string(raws[primaryKeyIndex]), 10, 0)
			if err != nil {
				log.Fatalf("get progress error: {%d} %+v", offset, err)
			}
		}
		c <- [2]string{strconv.FormatUint(offset, 10), strconv.FormatUint(uint64(crc32.ChecksumIEEE(total)), 10)}
		if offset == maxID {
			log.Printf("finished scan %s at: {%d}", table, offset)
			return
		}
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
