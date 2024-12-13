package kvstore

type KvStore struct {
	wal wal
}

func NewKvStore() *KvStore {
	store := KvStore{
		wal: *newWal(),
	}

	err := store.wal.loadHashIndex()
	if err != nil {
		panic(err)
	}

	return &store
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
	return nil
}

func (store *KvStore) Get(key string) *string {
	entry := store.wal.GetEntry(key)
	if entry == nil {
		return nil
	}

	_, value := entry.keyValue()
	return value
}

func (store *KvStore) Delete(key string) error {
	command := DeleteValueCommand{
		Key: key,
	}
	walEntry, err := command.toWalEntry()
	if err != nil {
		return err
	}

	store.wal.WriteEntry(&walEntry)
	return nil
}
