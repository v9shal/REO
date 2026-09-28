package main

import (
	"bufio"
	"strconv"
)

func handleSet(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 3 {
		writer.WriteString("-ERR wrong number of arguments for 'set' command\r\n")
		return
	}

	store.Set(args[1], args[2])
	writer.WriteString("+OK\r\n")
}

func handleGet(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 2 {
		writer.WriteString("-ERR wrong number of arguments for 'get' command\r\n")
		return
	}

	val, ok := store.Get(args[1])
	if !ok {
		writer.WriteString("$-1\r\n")
		return
	}

	// Zero reflection: write bulk string directly
	writer.WriteString("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n")
}

func handleDel(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 2 {
		writer.WriteString("-ERR wrong number of arguments for 'del' command\r\n")
		return
	}

	count := store.Del(args[1])
	writer.WriteString(":" + strconv.Itoa(count) + "\r\n")
}

func handleExists(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 2 {
		writer.WriteString("-ERR wrong number of arguments for 'exists' command\r\n")
		return
	}

	count := store.Exist(args[1])
	writer.WriteString(":" + strconv.Itoa(count) + "\r\n")
}

func handleExpire(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 3 {
		writer.WriteString("-ERR wrong number of arguments for 'expire' command\r\n")
		return
	}

	seconds, err := strconv.Atoi(args[2])
	if err != nil {
		writer.WriteString("-ERR value is not an integer or out of range\r\n")
		return
	}

	result := store.Expire(args[1], seconds)
	writer.WriteString(":" + strconv.Itoa(result) + "\r\n")
}

func handleTTL(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 2 {
		writer.WriteString("-ERR wrong number of arguments for 'ttl' command\r\n")
		return
	}

	result := store.TTL(args[1])
	writer.WriteString(":" + strconv.Itoa(result) + "\r\n")
}
