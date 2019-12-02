package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
	"github.com/vic/vic_go/utils"
)

type DB struct {
	*sql.DB
}

func connectPostgresqlDatabase(dbHost, username, password, dbName string) (dbObject *DB, err error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		username,
		password,
		dbHost,
		dbName))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(40)
	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func main() {
	dbHost := flag.String("h", "127.0.0.1:5432", "Database host")
	username := flag.String("u", "apple", "Database username")
	password := flag.String("p", "", "Database password")
	dbName := flag.String("d", "apple", "Database name")
	flag.Parse()
	start := time.Now()
	fmt.Printf("Connecting to database %s ....\n", *dbName)
	db, err := connectPostgresqlDatabase(*dbHost, *username, *password, *dbName)
	if err != nil {
		fmt.Println("Error connect DB: ", err.Error())
	}

	// step create table log
	// db.insert()
	db.run()
	elapsed := time.Since(start)
	fmt.Printf("----->  run with time: %s\n", elapsed)
	select {}
}

func (db *DB) run() {
	go db.insertOne()
	go db.updateOne()
	go db.deleteOne()
}

func (db *DB) insert() {
	for i := 0; i < 100000; i++ {
		_, err := db.Exec(`INSERT INTO whitelabel_build_tools (whitelabel_code, whitelabel_name) VALUES ($1 , $2)`, RandStringBytesMask(10), RandStringBytesMask(20))
		if err != nil {
			fmt.Println("Error Insert DB: ", err.Error())
		}
	}
}

func (db *DB) insertOne() {
	fmt.Println("--- insert ---")
	_, err := db.Exec(`INSERT INTO whitelabel_build_tools (whitelabel_code, whitelabel_name) VALUES ($1 , $2)`, RandStringBytesMask(10), RandStringBytesMask(20))
	fmt.Println(err)
	utils.DelayInDuration(5 * time.Second)
	go db.insertOne()
}

func (db *DB) updateOne() {
	fmt.Println("--- update ---")
	_, err := db.Exec(`UPDATE whitelabel_build_tools SET whitelabel_code= $1 , whitelabel_name = $2 WHERE id= $3`, RandStringBytesMask(10), RandStringBytesMask(20), rand.Intn(10000))
	fmt.Println(err)
	utils.DelayInDuration(5 * time.Second)
	go db.updateOne()
}

func (db *DB) deleteOne() {
	fmt.Println("--- delete ---")
	_, err := db.Exec(`DELETE FROM whitelabel_build_tools WHERE id= $1`, rand.Intn(10000))
	fmt.Println(err)
	utils.DelayInDuration(20 * time.Second)
	go db.deleteOne()
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
)

func RandStringBytesMask(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; {
		if idx := int(rand.Int63() & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i++
		}
	}
	return string(b)
}
