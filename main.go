package main

import (
	"fmt"
	"keyvault/kvstore"
	"sync"
)

func main() {
	store := kvstore.NewKvStore()
	var wg sync.WaitGroup

	putValue := func(key string, value string) {
		store.Put(key, value)
		wg.Done()
	}
	wg.Add(3)

	go putValue("name", "hameed")
	go putValue("name", "umaima")
	go putValue("name", "aneesa")

	wg.Wait()
	name := store.Get("name")
	fmt.Println(name)

}
