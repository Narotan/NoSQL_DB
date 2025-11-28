package mainq

import (
	"log"
	"nosql_db/internal/config"
	"nosql_db/internal/server"
)

func main() {
	cfg := config.Load()

	srv := server.New(cfg.Host + ":" + cfg.Port)

	if err := srv.Run(); err != nil {
		log.Fatal(err)
	}
}
