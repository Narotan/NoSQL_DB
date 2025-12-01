package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"nosql_db/internal/api"
	"nosql_db/internal/query"
	"os"
	"strings"
)

var (
	host = flag.String("host", "localhost", "Server host address")
	port = flag.String("port", "8080", "Server port")
)

func main() {
	flag.Parse()

	addr := fmt.Sprintf("%s:%s", *host, *port)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to server %s: %v", addr, err)
	}
	defer conn.Close()

	log.Printf("Connected to server %s", addr)

	runREPL(conn)
}

func runREPL(conn net.Conn) {
	reader := bufio.NewReader(os.Stdin)
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	fmt.Println("\nAvailable commands: INSERT, FIND, DELETE, CREATE_INDEX")
	fmt.Print("> ")

	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Error reading input: %v", err)
			continue
		}

		line := strings.TrimSpace(input)
		if line == "" {
			fmt.Print("> ")
			continue
		}

		if strings.EqualFold(line, "quit") || strings.EqualFold(line, "exit") {
			return
		}

		req, err := parseLineToRequest(line)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			fmt.Print("> ")
			continue
		}

		if err := encoder.Encode(req); err != nil {
			log.Fatalf("Error encoding request: %v", err)
		}

		var resp api.Response
		if err := decoder.Decode(&resp); err != nil {
			log.Fatalf("Error decoding response: %v", err)
		}

		printResponse(resp)
		fmt.Print("> ")
	}
}

func parseLineToRequest(line string) (*api.Request, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return nil, fmt.Errorf("invalid command format")
	}

	cmd := strings.ToUpper(fields[0])
	collectionName := fields[1]

	req := &api.Request{
		Database: collectionName,
		Command:  strings.ToLower(cmd),
	}

	if cmd == "CREATE_INDEX" {
		if len(fields) < 3 {
			return nil, fmt.Errorf("usage: CREATE_INDEX <collection> <field_name>")
		}
		fieldName := fields[2]
		req.Query = map[string]any{
			fieldName: nil,
		}
		return req, nil
	}

	if len(fields) < 3 {
		return nil, fmt.Errorf("missing JSON payload")
	}

	jsonPayload := strings.Join(fields[2:], " ")

	if cmd == "INSERT" {
		doc, err := query.ParseDocument(jsonPayload)
		if err != nil {
			return nil, fmt.Errorf("invalid JSON document: %v", err)
		}
		req.Data = []map[string]any{doc}
		return req, nil
	}

	q, err := query.Parse(jsonPayload)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON query: %v", err)
	}
	req.Query = q.Conditions
	return req, nil
}

func printResponse(resp api.Response) {
	if resp.Status == api.StatusError {
		fmt.Printf("ERROR: %s\n", resp.Message)
		return
	}

	fmt.Printf("SUCCESS: %s (Count: %d)\n", resp.Message, resp.Count)

	if len(resp.Data) > 0 {
		output, err := json.MarshalIndent(resp.Data, "", "  ")
		if err != nil {
			fmt.Printf("Warning: Failed to format results: %v\n", err)
		}
		fmt.Println(string(output))
	}
}
