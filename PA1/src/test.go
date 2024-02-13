package main

import (
	"encoding/hex"
	"fmt"
)

func stringToByte(s string) []byte {
	length := len(s)
	res := make([]byte, length/2)
	res, _ = hex.DecodeString(s)
	return res
}

func main() {
	s := "53FFBD1739202D4A4894"
	data := make([]byte, 10)
	data, _ = hex.DecodeString(s)
	fmt.Println(string(data))
	fmt.Println(fmt.Sprintf("%x", stringToByte(s)))
}
