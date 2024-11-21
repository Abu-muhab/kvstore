package kvstore

import (
	"os"
	"sort"
	"strconv"
	"strings"
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
	Entries        []walEntry
	sortedSegments []*walSegment
	openSegment    *walSegment
}

func newWal() *wal {
	return &wal{}
}

func (wal *wal) readSegments() {
	var segments []*walSegment = []*walSegment{}

	entries, _ := os.ReadDir("dat")
	for _, entry := range entries {
		nameAndExtension := strings.Split(entry.Name(), ".")

		var ext string
		if len(nameAndExtension) >= 2 {
			ext = nameAndExtension[1]
		}

		if ext == "wal" {
			nameParts := strings.Split(nameAndExtension[0], "_")
			segmentIndex, err := strconv.ParseUint(nameParts[2], 10, 64)
			if err != nil {
				panic(err)
			}
			segments = append(segments, newWalSegment(segmentIndex, nil))
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].meta.SegmentIndex < segments[j].meta.SegmentIndex
	})

	if len(segments) > 0 {
		wal.openSegment = segments[len(segments)-1]
		wal.sortedSegments = segments
	} else {
		openSegment := *newWalSegment(0, nil)
		wal.openSegment = &openSegment
		wal.sortedSegments = []*walSegment{&openSegment}
	}
}

func (wal *wal) maybeRoll() {
	if wal.openSegment.meta.isAtCapacity() || wal.openSegment.meta.Closed {
		wal.openSegment.close()
		newSegment := newWalSegment(wal.openSegment.meta.SegmentIndex+1, wal.openSegment)
		wal.sortedSegments = append(wal.sortedSegments, newSegment)
		wal.openSegment = newSegment
	}
}

func (wal *wal) WriteEntry(entry *walEntry) error {
	wal.maybeRoll()
	err := wal.openSegment.writeEntry(entry)
	if err == nil {
		wal.Entries = append(wal.Entries, *entry)
	}
	return err
}

func (wal *wal) ReadEntries() error {
	wal.readSegments()

	entries := []walEntry{}
	for _, segment := range wal.sortedSegments {
		segmentEntries, err := segment.readEntries()
		if err != nil {
			return err
		}
		entries = append(entries, segmentEntries...)
	}

	wal.Entries = entries
	return nil
}
