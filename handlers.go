package main

import (
	"fmt"
	"net"
	"strconv"
)

func handleSet(conn net.Conn, store *MemoryStore, args []string) {
	if len(args) != 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		return
	}

	key := args[1]
	val := args[2]

	store.Set(key, val)
	conn.Write([]byte("+OK\r\n"))
}

func handleGet(conn net.Conn, store *MemoryStore, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return
	}

	key := args[1]
	val, ok := store.Get(key)
	if !ok {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
	conn.Write([]byte(response))
}

func handleDel(conn net.Conn, store *MemoryStore, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
		return
	}

	key := args[1]
	count := store.Del(key)

	response := fmt.Sprintf(":%d\r\n", count)
	conn.Write([]byte(response))
}

func handleExists(conn net.Conn, store *MemoryStore, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'exists' command\r\n"))
		return
	}

	key := args[1]
	count := store.Exist(key)

	response := fmt.Sprintf(":%d\r\n", count)
	conn.Write([]byte(response))
}

func handleExpire(conn net.Conn, store *MemoryStore, args []string) {
	if len(args) != 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'Expire' command\r\n"))
		return
	}
	key := args[1]
	time := args[2]
	seconds, error := strconv.Atoi(time)
	if error != nil {
		conn.Write([]byte("-Err while parsing seconds to int\r\n"))
		return
	}
	result := store.Expire(key, seconds)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", result)))
}
func handleTTL(conn net.Conn, store *MemoryStore, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'Expire' command\r\n"))
		return
	}
	key := args[1]
	result := store.TTL(key)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", result)))

}
