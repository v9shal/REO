package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, errors.New("expected '*' at start of command")
	}
	arrayCount, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}

	var args []string
	for i := 0; i < arrayCount; i++ {
		// Read the length line (e.g. "$4\r\n")
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSuffix(lenLine, "\r\n")

		if len(lenLine) == 0 || lenLine[0] != '$' {
			return nil, errors.New("expected '$' for bulk string length")
		}

		// Convert "$4" -> 4
		strLen, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %v", err)
		}

		// Read exactly that many bytes from the stream (e.g. "PING")
		strBytes := make([]byte, strLen)
		_, err = io.ReadFull(reader, strBytes)
		if err != nil {
			return nil, err
		}

		// Read and discard the trailing "\r\n" after the string
		// Every bulk string has 2 bytes (\r and \n) after its data
		discardBuf := make([]byte, 2)
		_, err = io.ReadFull(reader, discardBuf)
		if err != nil {
			return nil, err
		}

		// Append the string to our arguments list
		args = append(args, string(strBytes))
	}

	return args, nil

}
