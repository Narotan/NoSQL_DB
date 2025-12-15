package main

import (
	"log"
	"nosql_db/internal/config"
	"nosql_db/internal/server"
)

func main() {
	log.Println("Starting NoSQLdb server...")
	cfg := config.Load()

	srv := server.New(cfg.Host + ":" + cfg.Port)

	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}
}
