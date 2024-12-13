package kvstore

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
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

func (entry *walEntry) keyValue() (*string, *string) {
	if entry.EntryType == WalEntryTypeSetCommand {
		command := SetValueCommand{}
		command.fromWalEntry(*entry)
		return &command.Key, &command.Value
	} else if entry.EntryType == WalEntryTypeDeleteCommand {
		command := DeleteValueCommand{}
		command.fromWalEntry(*entry)
		return &command.Key, nil
	}
	return nil, nil
}

type walMetadata struct {
	SortedSegmentsMetadata []*walSegmentMetadata `json:"sortedSegmentsMetadata"`
}

func (meta *walMetadata) sortMetadata() {
	sort.Slice(meta.SortedSegmentsMetadata, func(i, j int) bool {
		if meta.SortedSegmentsMetadata[i].SegmentIndex != meta.SortedSegmentsMetadata[j].SegmentIndex {
			return meta.SortedSegmentsMetadata[i].SegmentIndex < meta.SortedSegmentsMetadata[j].SegmentIndex
		}
		return meta.SortedSegmentsMetadata[i].CreatedAt.Before(meta.SortedSegmentsMetadata[j].CreatedAt)
	})
}

type wal struct {
	sortedSegments       []*walSegment
	openSegment          *walSegment
	metatada             *walMetadata
	segmentCleanupTicker *time.Ticker
	cleaningSegments     bool
	segmentCleanupMutex  sync.Mutex
}

func newWal() *wal {
	wal := &wal{}
	wal.startCleanupTicker()
	return wal
}

func (wal *wal) metaPath() string {
	return filepath.Join("dat", "meta", "wal_metadata.dat")
}

func (wal *wal) newMetadata(segmentIndex uint64, firstEntryIndex uint64) *walSegmentMetadata {
	metadata := &walSegmentMetadata{
		SegmentIndex: segmentIndex,
	}

	metadata.FirstEntryIndex = firstEntryIndex
	metadata.LastEntryIndex = firstEntryIndex
	metadata.CreatedAt = time.Now()
	metadata.Id = uuid.NewString()

	return metadata
}

func (wal *wal) openNewSegment(index uint64, previousSegment *walSegment) *walSegment {
	if wal.openSegment != nil {
		wal.openSegment.close()
	}

	var firstEntryIndex uint64 = 0
	if previousSegment != nil {
		firstEntryIndex = previousSegment.meta.LastEntryIndex
	}

	meta := wal.newMetadata(index, firstEntryIndex)
	segment := newWalSegment(meta)

	wal.sortedSegments = append(wal.sortedSegments, segment)
	wal.metatada.SortedSegmentsMetadata = append(wal.metatada.SortedSegmentsMetadata, meta)
	wal.openSegment = segment

	return segment
}

func (wal *wal) deleteSegment(segmentId string) {
	//get the segment meta segmentMetadataIndex
	segmentMetadataIndex := 0
	var segmentToDelete *walSegment
	for i, m := range wal.metatada.SortedSegmentsMetadata {
		if m.Id == segmentId {
			segmentMetadataIndex = i
			segmentToDelete = newWalSegment(m)
			break
		}
	}

	//remove it from the metadata
	sortedSegments := wal.metatada.SortedSegmentsMetadata
	wal.metatada.SortedSegmentsMetadata = append(sortedSegments[:segmentMetadataIndex], sortedSegments[segmentMetadataIndex+1:]...)

	//delete the segment file
	segmentToDelete.meta.deleteSegmentLogFile()

	//save the updated metadata without the meta of the deleted segment
	wal.metatada.sortMetadata()
	wal.saveMetadata()
}

func (wal *wal) getNextDirtySegment() *walSegmentMetadata {
	wal.loadMetadata()
	var value *walSegmentMetadata

	for _, meta := range wal.metatada.SortedSegmentsMetadata {
		if !meta.IsCompactedSegment {
			value = meta
			break
		}
	}

	return value
}

func (wal *wal) saveMetadata() {
	wal.metatada.sortMetadata()
	data, err := json.Marshal(wal.metatada)
	if err != nil {
		panic(err)
	}

	os.MkdirAll("dat/meta", 0755)
	os.WriteFile(wal.metaPath(), data, 0644)
}

func (wal *wal) loadMetadata() {
	bytes, err := os.ReadFile(wal.metaPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		panic(err)
	}

	err = json.Unmarshal(bytes, &wal.metatada)
	if err != nil {
		panic(err)
	}

	wal.metatada.sortMetadata()
}

func (wal *wal) readSegments() {
	wal.loadMetadata()

	if wal.metatada == nil {
		wal.metatada = &walMetadata{
			SortedSegmentsMetadata: []*walSegmentMetadata{},
		}
	}

	var segments []*walSegment = []*walSegment{}

	for _, meta := range wal.metatada.SortedSegmentsMetadata {
		segments = append(segments, newWalSegment(meta))
	}

	if len(segments) > 0 {
		wal.openSegment = segments[len(segments)-1]
		wal.sortedSegments = segments
	} else {
		wal.openNewSegment(0, nil)
	}
}

func (wal *wal) maybeRoll() {
	if wal.openSegment.meta.isAtCapacity() || wal.openSegment.meta.Closed {
		wal.openNewSegment(wal.openSegment.meta.SegmentIndex+1, wal.openSegment)
	}
}

func (wal *wal) WriteEntry(entry *walEntry) error {
	wal.maybeRoll()
	err := wal.openSegment.writeEntry(entry, nil)

	if err == nil {
		wal.saveMetadata()
	}
	return err
}

func (wal *wal) GetEntry(key string) *walEntry {
	count := len(wal.sortedSegments)
	for i := count - 1; i >= 0; i-- {
		segment := wal.sortedSegments[i]
		offset, exists := segment.hashIndex[key]
		if !exists {
			continue
		}

		entry := segment.ReadEntryAtOffset(offset)
		if entry == nil {
			continue
		}

		return entry
	}

	return nil
}

func (wal *wal) loadHashIndex() error {
	wal.readSegments()

	for _, segment := range wal.sortedSegments {
		if segment.meta.IsCompactedSegment && !segment.meta.CompactionCompleted {
			continue
		}

		err := segment.loadHashIndex()
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (wal *wal) startCleanupTicker() {
	ticker := time.NewTicker(1 * time.Minute)

	if wal.segmentCleanupTicker != nil {
		wal.segmentCleanupTicker.Stop()
	}

	wal.segmentCleanupTicker = ticker

	go func() {
		for range ticker.C {
			wal.cleanSegments()
		}
	}()
}

func (wal *wal) cleanSegments() {
	if wal.cleaningSegments {
		return
	}
	wal.segmentCleanupMutex.Lock()
	wal.cleaningSegments = true

	defer wal.segmentCleanupMutex.Unlock()
	defer func() { wal.cleaningSegments = false }()

	offsetMap := make(map[string]*uint64)
	cleanedSegments := []*walSegment{}
	segmentToClean := newWalSegment(wal.getNextDirtySegment())

	if segmentToClean == nil || !segmentToClean.meta.Closed {
		return
	}

	segmentToClean.processEntries(func(entry walEntry) {
		key, _ := entry.keyValue()
		if key == nil {
			return
		}

		offset, exists := offsetMap[*key]
		if !exists || entry.Index > *offset {
			offsetMap[*key] = &entry.Index
		}
	})

	entries := []walEntry{}

	for _, meta := range wal.metatada.SortedSegmentsMetadata {
		segment := newWalSegment(meta)

		if segmentToClean.meta.SegmentIndex >= segment.meta.SegmentIndex && segment.meta.Closed {
			segment.processEntries(func(entry walEntry) {
				key, value := entry.keyValue()
				if key == nil {
					return
				}

				offset, exists := offsetMap[*key]
				if !exists && value != nil {
					entries = append(entries, entry)
				} else if entry.Index >= *offset {
					if value == nil {
						tempEntries := []walEntry{}
						for _, e := range entries {
							k, _ := e.keyValue()
							if key != k {
								tempEntries = append(tempEntries, e)
							}
						}
						entries = tempEntries
					} else {
						entries = append(entries, entry)
					}
				}
			})

			cleanedSegments = append(cleanedSegments, segment)
		}

		if segment.meta.Id == segmentToClean.meta.Id {
			break
		}
	}

	var newSegmentMetas []*walSegmentMetadata

	batchSize := walSegmentSize
	currentBatchSize := 0

	var currentSegment *walSegment
	var currentSegmentMeta *walSegmentMetadata

	for index, entry := range entries {
		if currentBatchSize == 0 {
			currentSegmentMeta = wal.newMetadata(uint64(len(newSegmentMetas)), entry.Index)
			currentSegmentMeta.IsCompactedSegment = true
			currentSegmentMeta.CompactionCompleted = false
			currentSegmentMeta.FirstEntryIndex = entry.Index
			currentSegment = newWalSegment(currentSegmentMeta)
			newSegmentMetas = append(newSegmentMetas, currentSegmentMeta)
		}

		currentSegment.writeEntry(&entry, &entry.Index)
		currentBatchSize += 1

		if currentBatchSize >= batchSize || index == len(entries)-1 {
			currentSegmentMeta.LastEntryIndex = entry.Index

			wal.metatada.SortedSegmentsMetadata = append(wal.metatada.SortedSegmentsMetadata, currentSegmentMeta)
			wal.saveMetadata()

			currentBatchSize = 0
		}
	}

	for _, meta := range newSegmentMetas {
		meta.CompactionCompleted = true
		meta.Closed = true
	}
	wal.saveMetadata()

	for _, segment := range cleanedSegments {
		wal.deleteSegment(segment.meta.Id)
	}
	wal.saveMetadata()
}
