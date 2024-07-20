package kvstore

type KvStore struct {
	wal wal
	kv  map[string]string
}

func NewKvStore() *KvStore {
	store := KvStore{
		wal: newWal(),
		kv:  make(map[string]string),
	}
	store.applyLog()
	return &store
}

func (store *KvStore) applyLog() {
	error := store.wal.ReadEntries()
	if error != nil {
		panic(error)
	}

	logEntries := store.wal.Entries

	for _, entry := range logEntries {
		if entry.EntryType == SetValueCommandType {
			command := SetValueCommand{}
			command.fromWalEntry(entry)
			store.kv[command.Key] = command.Value
		}
	}
}

func (store *KvStore) Put(key string, value string) error {
	command := SetValueCommand{
		Key:   key,
		Value: value,
	}
	walEntry, err := command.toWalEntry()
	if err != nil {
		return err
	}

	store.wal.WriteEntry(&walEntry)
	store.kv[key] = value
	return nil
}

func (store *KvStore) Get(key string) string {
	return store.kv[key]
}
