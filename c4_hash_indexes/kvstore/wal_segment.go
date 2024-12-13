package kvstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const walSegmentSize = 5

type walSegmentMetadata struct {
	FirstEntryIndex     uint64    `json:"firstEntryIndex"`
	LastEntryIndex      uint64    `json:"lastEntryIndex"`
	Closed              bool      `json:"closed"`
	SegmentIndex        uint64    `json:"segmentIndex"`
	CreatedAt           time.Time `json:"createdAt"`
	Id                  string    `json:"id"`
	IsCompactedSegment  bool      `json:"isCompactedSegment"`
	CompactionCompleted bool      `json:"compactionCompleted"`
}

type walSegment struct {
	meta       *walSegmentMetadata
	file       *os.File
	fileWriter *bufio.Writer
	writeMutex sync.Mutex
	hashIndex  map[string]int64
}

func newWalSegment(meta *walSegmentMetadata) *walSegment {
	if meta == nil {
		return nil
	}

	segment := walSegment{
		meta:      meta,
		hashIndex: make(map[string]int64),
	}

	return &segment
}

func (meta *walSegmentMetadata) segmentLogFilePath() string {
	return filepath.Join("dat", fmt.Sprintf("wal_segment_%d_%s.wal", meta.SegmentIndex, meta.Id))
}

func (meta *walSegmentMetadata) deleteSegmentLogFile() error {
	return os.Remove(meta.segmentLogFilePath())
}

func (meta *walSegmentMetadata) isAtCapacity() bool {
	if meta.LastEntryIndex < meta.FirstEntryIndex {
		return false
	}
	return (meta.LastEntryIndex - meta.FirstEntryIndex) >= walSegmentSize
}

func (walSegment *walSegment) ReadEntryAtOffset(offset int64) *walEntry {
	file, err := os.Open(walSegment.meta.segmentLogFilePath())
	if err != nil {
		return nil
	}

	file.Seek(offset, io.SeekStart)
	reader := bufio.NewReader(file)
	bytes, err := reader.ReadBytes('\n')
	if err != nil {
		return nil
	}

	var entry *walEntry = &walEntry{}
	err = json.Unmarshal(bytes, entry)
	if err != nil {
		panic(err)
	}

	return entry
}

func (walSegment *walSegment) writeEntry(entry *walEntry, index *uint64) error {
	if walSegment.meta.Closed {
		panic("cannot write to a closed segment")
	}

	if walSegment.meta.segmentLogFilePath() == "" {
		panic("invalid walSegment filename")
	}

	if walSegment.meta.isAtCapacity() {
		panic("current wal segment at capacity")
	}

	walSegment.writeMutex.Lock()
	defer walSegment.writeMutex.Unlock()

	if walSegment.file == nil || walSegment.fileWriter == nil {
		file, err := os.OpenFile(walSegment.meta.segmentLogFilePath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}
		dbWriter := bufio.NewWriter(file)

		walSegment.file = file
		walSegment.fileWriter = dbWriter
	}

	if index != nil {
		entry.Index = *index
	} else {
		entry.Index = walSegment.meta.LastEntryIndex
	}

	//get the offset the new entry is to be written to
	offset, err := walSegment.file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}

	//write the entry to file
	bytes, _ := json.Marshal(entry)
	_, err = walSegment.fileWriter.Write(append(bytes, '\n'))
	walSegment.fileWriter.Flush()

	if err == nil {
		if index == nil {
			walSegment.meta.LastEntryIndex++
		}
	}

	//update the hash index with the offset of the new entry
	k, _ := entry.keyValue()
	walSegment.hashIndex[*k] = offset

	return err
}

func (walSegment *walSegment) loadHashIndex() error {
	os.Mkdir("dat", 0755)
	file, err := os.Open(walSegment.meta.segmentLogFilePath())

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}

	reader := bufio.NewReader(file)
	diskOffeset := int64(0)

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

		key, _ := entry.keyValue()

		walSegment.hashIndex[*key] = diskOffeset
		diskOffeset += int64(len(bytes))
	}

	return nil
}

type EntryOperation func(entry walEntry)

func (walSegment *walSegment) processEntries(operation EntryOperation) {
	os.Mkdir("dat", 0755)
	file, err := os.Open(walSegment.meta.segmentLogFilePath())

	if err != nil {
		return
	}

	reader := bufio.NewReader(file)

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
		operation(entry)
	}
}

func (walSegment *walSegment) close() {
	if walSegment.fileWriter != nil {
		walSegment.fileWriter.Flush()
		walSegment.file.Close()
	}
	walSegment.meta.Closed = true
}
