package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	store := NewMemoryStore()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating listener: %v\n", err)
	}

	log.Println("Redis server listening on port 8080...")

	// 1. Create a channel to listen for OS signals (buffer size 1 is standard)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// 2. Spawn a background goroutine waiting specifically for Ctrl+C
	go func() {
		sig := <-shutdown
		log.Printf("\nReceived signal %v. Shutting down server...\n", sig)

		// Closing the listener causes listener.Accept() to unblock immediately!
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Listener closed. Exiting server loop.")
				break
			}
			log.Printf("error accepting connection: %v\n", err)
			continue
		}

		go handleConnection(conn, store)
	}

	log.Println("Server shut down successfully.")
}

func handleConnection(conn net.Conn, store *MemoryStore) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := parseRESP(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("Client disconnected cleanly: %s\n", conn.RemoteAddr())
				return
			}
			log.Printf("Parser error from %s: %v\n", conn.RemoteAddr(), err)
			return
		}
		log.Printf("Parsed command: %v\n", args)
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			if len(args) == 1 {
				conn.Write([]byte("+PONG\r\n"))
			} else if len(args) == 2 {
				msg := args[1]
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'ping' command\r\n"))
			}
		case "GET":
			handleGet(conn, store, args)
		case "SET":
			handleSet(conn, store, args)
		case "DEL":
			handleDel(conn, store, args)
		case "EXISTS":
			handleExists(conn, store, args)
		default:
			errMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
			conn.Write([]byte(errMsg))

		}
	}
}
