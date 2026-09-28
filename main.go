package main

import (
	"bufio"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	store := NewShardStore()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating listener: %v\n", err)
	}

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

func handleConnection(conn net.Conn, store *ShardedStore) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn) // 🚀 Buffered output: coalesces TCP packets

	for {
		args, err := parseRESP(reader)
		if err != nil {
			return // Disconnect cleanly, no logging on hot path
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			if len(args) == 1 {
				writer.WriteString("+PONG\r\n")
			} else {
				msg := args[1]
				writer.WriteString("$" + strconv.Itoa(len(msg)) + "\r\n" + msg + "\r\n")
			}
		case "SET":
			handleSet(writer, store, args)
		case "GET":
			handleGet(writer, store, args)
		case "DEL":
			handleDel(writer, store, args)
		case "EXISTS":
			handleExists(writer, store, args)
		case "EXPIRE":
			handleExpire(writer, store, args)
		case "TTL":
			handleTTL(writer, store, args)
		default:
			writer.WriteString("-ERR unknown command '" + args[0] + "'\r\n")
		}

		// Flush all buffered bytes in ONE single TCP syscall!
		writer.Flush()
	}
}
