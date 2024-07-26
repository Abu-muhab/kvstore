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
			EntryType: WalEntryTypeSetCommand,
		}, nil
	}

	return walEntry{}, err
}

func (command *SetValueCommand) fromWalEntry(entry walEntry) error {
	if entry.EntryType != WalEntryTypeSetCommand {
		return ErrWrongWalEntryType
	}

	err := json.Unmarshal(entry.Data, command)
	if err != nil {
		return err
	}

	return nil
}

type DeleteValueCommand struct {
	Key string `json:"key"`
}

func (command *DeleteValueCommand) toWalEntry() (walEntry, error) {
	marshaled, err := json.Marshal(command)

	if err == nil {
		return walEntry{
			Data:      marshaled,
			EntryType: WalEntryTypeDeleteCommand,
		}, nil
	}

	return walEntry{}, err
}

func (command *DeleteValueCommand) fromWalEntry(entry walEntry) error {
	if entry.EntryType != WalEntryTypeDeleteCommand {
		return ErrWrongWalEntryType
	}

	err := json.Unmarshal(entry.Data, command)
	if err != nil {
		return err
	}

	return nil
}
