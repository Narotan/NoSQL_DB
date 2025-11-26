package main

import (
	"log"
	"nosql_db/internal/handlers"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		handlers.PrintUsage()
		log.Fatal()
	}

	dbName := os.Args[1]
	command := os.Args[2]

	var jsonQuery string
	if len(os.Args) >= 4 {
		jsonQuery = os.Args[3]
	}

	if err := handlers.ExecuteCommand(dbName, command, jsonQuery); err != nil {
		log.Fatal(err)
	}
}
