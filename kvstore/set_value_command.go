package kvstore

import (
	"encoding/json"
	"errors"
)

var ErrWrongWalEntryType = errors.New("unexpected Wal entry type")

type SetValueCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (command *SetValueCommand) toWalEntry() (walEntry, error) {
	marshaled, err := json.Marshal(command)

	if err == nil {
		return walEntry{
			Data:      marshaled,
			EntryType: SetValueCommandType,
		}, nil
	}

	return walEntry{}, err
}

func (command *SetValueCommand) fromWalEntry(entry walEntry) error {
	if entry.EntryType != SetValueCommandType {
		return ErrWrongWalEntryType
	}

	err := json.Unmarshal(entry.Data, command)
	if err != nil {
		return err
	}

	return nil
}
