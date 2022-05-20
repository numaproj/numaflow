package main

import (
	"net/http"
)

func main() {
	if err := http.ListenAndServe(":8378", nil); err != nil {
		panic(err)
	}
}
