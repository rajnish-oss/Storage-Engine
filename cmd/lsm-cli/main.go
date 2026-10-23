package main

import (
	"fmt"

	"github.com/rajnish-oss/LSM_storage_engine/pkg/lsm"
)

func main() {
	db, err := lsm.NewDB[string, string]()
	if err != nil {
		panic(err)
	}

	db.Put("a", "apple")
	value, ok := db.Get("a")
	fmt.Printf("value=%q found=%t\n", value, ok)
}
