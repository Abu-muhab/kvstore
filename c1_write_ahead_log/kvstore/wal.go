package kvstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

type WalEntryType int

const (
	WalEntryTypeSetCommand = iota
	WalEntryTypeDeleteCommand
)

type walEntry struct {
	Index     uint64       `json:"index"`
	Data      []byte       `json:"data"`
	EntryType WalEntryType `json:"entryType"`
}

type wal struct {
	Entries    []walEntry
	file       *os.File
	writeMutex sync.Mutex
}

const walFileName string = "kvwal.wal"

func newWal() wal {
	return wal{}
}

func (wal *wal) WriteEntry(entry *walEntry) {
	wal.writeMutex.Lock()
	defer wal.writeMutex.Unlock()

	var index uint64 = 0
	if len(wal.Entries) > 0 {
		index = uint64(len(wal.Entries))
	}
	entry.Index = index

	if wal.file == nil {
		file, err := os.OpenFile(walFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		wal.file = file
	}
	dbWriter := bufio.NewWriter(wal.file)

	bytes, _ := json.Marshal(entry)
	_, err := dbWriter.Write(append(bytes, '\n'))
	dbWriter.Flush()

	if err == nil {
		wal.Entries = append(wal.Entries, *entry)
	}
}

func (wal *wal) ReadEntries() error {
	file, err := os.Open(walFileName)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	reader := bufio.NewReader(file)
	var entries = []walEntry{}

	for {
		bytes, err := reader.ReadBytes('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				panic(err)
			}
			break
		}

		var entry walEntry
		json.Unmarshal(bytes, &entry)
		entries = append(entries, entry)
	}

	wal.Entries = entries
	return nil
}
