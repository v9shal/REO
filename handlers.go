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

func handleHistory(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 2 {
		writer.WriteString("-ERR wrong number of arguments for 'history' command\r\n")
		return
	}

	key := args[1]
	history := store.History(key)

	if len(history) == 0 {
		writer.WriteString("*0\r\n") // Empty RESP Array
		return
	}

	// 1. Write the Array Header: *<count>\r\n
	writer.WriteString("*" + strconv.Itoa(len(history)) + "\r\n")

	// 2. Write each version as a formatted Bulk String
	for _, v := range history {
		var entry string
		if v.IsTombStone {
			entry = "v" + strconv.FormatUint(v.VersionId, 10) + " | " + strconv.FormatInt(v.Timestamp, 10) + " | [DELETED]"
		} else {
			entry = "v" + strconv.FormatUint(v.VersionId, 10) + " | " + strconv.FormatInt(v.Timestamp, 10) + " | " + v.Value
		}

		// Write Bulk String for this entry
		writer.WriteString("$" + strconv.Itoa(len(entry)) + "\r\n" + entry + "\r\n")
	}
}
func handleEvict(writer *bufio.Writer, store *ShardedStore, args []string) {
	if len(args) != 2 {
		writer.WriteString("-ERR wrong number of arguments for 'evict' command\r\n")
		return
	}

	key := args[1]
	err := store.Evict(key)
	if err != nil {
		writer.WriteString("-ERR eviction failed: " + err.Error() + "\r\n")
		return
	}

	writer.WriteString("+OK\r\n")
}
func handleAsOf(writer *bufio.Writer, store *ShardedStore, args []string) {
	// Syntax: AS.OF <key> <timestamp_nano>
	if len(args) != 3 {
		writer.WriteString("-ERR wrong number of arguments for 'as.of' command\r\n")
		return
	}

	key := args[1]
	targetTimeNano, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		writer.WriteString("-ERR timestamp must be a valid 64-bit integer\r\n")
		return
	}

	val, ok := store.AsOf(key, targetTimeNano)
	if !ok {
		writer.WriteString("$-1\r\n") // Not found / tombstone / expired at that time
		return
	}

	writer.WriteString("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n")
}

func handleRollback(writer *bufio.Writer, store *ShardedStore, args []string) {
	// Syntax: ROLLBACK <key> <version_id>
	if len(args) != 3 {
		writer.WriteString("-ERR wrong number of arguments for 'rollback' command\r\n")
		return
	}

	key := args[1]
	targetVID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		writer.WriteString("-ERR version id must be an integer\r\n")
		return
	}

	_, ok := store.Rollback(key, targetVID)
	if !ok {
		writer.WriteString("-ERR version not found or target was deleted\r\n")
		return
	}

	writer.WriteString("+OK\r\n")
}
