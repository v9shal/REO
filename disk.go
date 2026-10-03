package main

import (
	"encoding/json"
	"os"
	"sync"
)

type DiskStub struct {
	Offset int64
	Length uint32
}

type DiskEngine struct {
	file   *os.File
	mu     sync.Mutex
	offset int64
}

func NewDiskEngine(filePath string) (*DiskEngine, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err

	}
	currentSize := stat.Size()
	return &DiskEngine{
		file:   file,
		offset: currentSize,
	}, nil
}

func (d *DiskEngine) Write(payload []byte) (DiskStub, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	startOffset := d.offset
	_, err := d.file.Write(payload)
	if err != nil {
		return DiskStub{}, err

	}

	d.offset += int64(len(payload))
	return DiskStub{
		Offset: startOffset,
		Length: uint32(len(payload)),
	}, nil
}

func (d *DiskEngine) Read(stub DiskStub) ([]byte, error) {
	buf := make([]byte, stub.Length)
	_, err := d.file.ReadAt(buf, stub.Offset)
	if err != nil {
		return buf, err
	}
	return buf, nil
}
func serializeVersions(versions []Version) ([]byte, error) {
	res, err := json.Marshal(versions)
	if err != nil {
		return nil, err

	}
	return res, nil
}
func deserializeVersions(data []byte) ([]Version, error) {
	var versions []Version

	if err := json.Unmarshal(data, &versions); err != nil {
		return nil, err
	}

	return versions, nil
}
