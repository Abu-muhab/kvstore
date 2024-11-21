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
)

const walSegmentSize = 50

type walSegmentMetadata struct {
	FirstEntryIndex uint64 `json:"firstEntryIndex"`
	LastEntryIndex  uint64 `json:"lastEntryIndex"`
	Closed          bool   `json:"closed"`
	SegmentIndex    uint64 `json:"segmentIndex"`
}

type walSegment struct {
	meta       walSegmentMetadata
	file       *os.File
	fileWriter *bufio.Writer
	writeMutex sync.Mutex
}

func newSegmentMetadata(index uint64) (walSegmentMetadata, error) {
	metadata := walSegmentMetadata{
		SegmentIndex: index,
	}

	data, err := os.ReadFile(metadata.segmentMetaFilePath())
	if err != nil {
		return metadata, err
	}

	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return metadata, err
	}

	return metadata, nil
}

func newWalSegment(segmentIndex uint64, prevSegment *walSegment) *walSegment {
	meta, _ := newSegmentMetadata(segmentIndex)
	if prevSegment != nil {
		meta.FirstEntryIndex = prevSegment.meta.LastEntryIndex
		meta.LastEntryIndex = prevSegment.meta.LastEntryIndex
	}

	segment := walSegment{
		meta: meta,
	}
	return &segment
}

func (meta *walSegmentMetadata) segmentLogFilePath() string {
	return filepath.Join("dat", fmt.Sprintf("wal_segment_%d.wal", meta.SegmentIndex))
}

func (meta *walSegmentMetadata) segmentMetaFilePath() string {
	return filepath.Join("dat", "meta", fmt.Sprintf("wal_segment_metadata_%d.walmeta", meta.SegmentIndex))
}

func (meta *walSegmentMetadata) isAtCapacity() bool {
	return (meta.LastEntryIndex - meta.FirstEntryIndex) >= walSegmentSize
}

func (walSegment *walSegment) writeEntry(entry *walEntry) error {
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

	entry.Index = walSegment.meta.LastEntryIndex + 1
	bytes, _ := json.Marshal(entry)
	_, err := walSegment.fileWriter.Write(append(bytes, '\n'))
	walSegment.fileWriter.Flush()

	if err == nil {
		walSegment.meta.LastEntryIndex++
		walSegment.saveMetadata()
	}

	return err
}

func (walSegment *walSegment) readEntries() ([]walEntry, error) {
	os.Mkdir("dat", 0755)
	file, err := os.Open(walSegment.meta.segmentLogFilePath())

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []walEntry{}, nil
		}

		return []walEntry{}, err
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

	return entries, nil
}

func (walSegment *walSegment) close() {
	walSegment.fileWriter.Flush()
	walSegment.file.Close()
	walSegment.meta.Closed = true
	walSegment.saveMetadata()
}

func (walSegment *walSegment) saveMetadata() {
	data, err := json.Marshal(walSegment.meta)
	if err != nil {
		panic(err)
	}

	os.MkdirAll("dat/meta", 0755)
	os.WriteFile(walSegment.meta.segmentMetaFilePath(), data, 0644)
}
