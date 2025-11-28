package server

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"nosql_db/internal/api"
	"nosql_db/internal/handlers"
)

type TCPServer struct {
	Address string
}

func New(address string) *TCPServer {
	return &TCPServer{Address: address}
}

func (s *TCPServer) Run() error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("Server running on %s", s.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Conn error: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	log.Printf("сlient connected: %s", clientAddr)

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var req api.Request
		err := decoder.Decode(&req)
		if err != nil {
			if err == io.EOF {
				log.Printf("сlient disconnected: %s", clientAddr)
			} else {
				log.Printf("decode error from %s: %v", clientAddr, err)
			}
			return
		}

		resp := handlers.HandleRequest(req)

		if err := encoder.Encode(resp); err != nil {
			log.Printf("encode error to %s: %v", clientAddr, err)
			return
		}
	}
}
