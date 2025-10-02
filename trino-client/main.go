package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	dsn := "http://user@localhost:8091?catalog=hive&schema=default"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Get all tables in the schema
	tableRows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		log.Fatal(err)
	}
	defer tableRows.Close()

	var tableName string
	tables := []string{}
	for tableRows.Next() {
		if err := tableRows.Scan(&tableName); err != nil {
			log.Fatal(err)
		}
		tables = append(tables, tableName)
	}

	if err := tableRows.Err(); err != nil {
		log.Fatal(err)
	}

	// Iterate over tables and print all rows with timing
	for _, tbl := range tables {
		start := time.Now()
		fmt.Printf("\n=== Table: %s ===\n", tbl)

		query := fmt.Sprintf("SELECT * FROM %s", tbl)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Printf("Failed to query table %s: %v\n", tbl, err)
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			values := make([]interface{}, len(cols))
			for i := range values {
				var v interface{}
				values[i] = &v
			}

			if err := rows.Scan(values...); err != nil {
				log.Fatal(err)
			}

			for i, col := range cols {
				val := *(values[i].(*interface{}))
				fmt.Printf("%s=%v ", col, val)
			}
			fmt.Println()
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		rows.Close()
		fmt.Printf("Duration for table %s: %s\n", tbl, time.Since(start))
	}

	fmt.Println("\nDone.")
}
