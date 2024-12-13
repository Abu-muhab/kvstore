package main

import (
	"encoding/json"
	"io"
	"keyvault/kvstore"
	"net/http"
)

var store *kvstore.KvStore = kvstore.NewKvStore()

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (request *PutRequest) isValid() bool {
	return request.Key != "" && request.Value != ""
}

func handleHttpError(w http.ResponseWriter, e error) {
	if e == nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), 400)
		return
	}
	http.Error(w, e.Error(), 400)
}

func httpHandler(w http.ResponseWriter, req *http.Request) {
	method := req.Method

	if method == http.MethodGet {
		query := req.URL.Query()
		key := query.Get("key")

		if key == "" {
			handleHttpError(w, nil)
			return
		}

		w.Header().Add("Content-Type", "application/json")
		encoder := json.NewEncoder(w)

		value := store.Get(key)
		encoder.Encode(map[string]string{
			"key": key,
			"value": func() string {
				if value == nil {
					return ""
				}
				return *value
			}(),
		})
	}

	if method == http.MethodPost {
		body, _ := io.ReadAll(req.Body)

		var request PutRequest
		err := json.Unmarshal(body, &request)
		if err != nil || !request.isValid() {
			handleHttpError(w, err)
			return
		}

		store.Put(request.Key, request.Value)
	}

	if method == http.MethodDelete {
		query := req.URL.Query()
		key := query.Get("key")

		if key == "" {
			handleHttpError(w, nil)
			return
		}

		store.Delete(key)
	}

}

func main() {
	http.HandleFunc("/", httpHandler)
	http.ListenAndServe(":8090", nil)
}
