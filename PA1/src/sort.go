package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func merge(keys1 [][]byte, values1 [][]byte, keys2 [][]byte, values2 [][]byte) ([][]byte, [][]byte) {
	pt1 := 0
	pt2 := 0
	pt := 0
	keys := make([][]byte, len(keys1)+len(keys2))
	values := make([][]byte, len(values1)+len(values2))
	for pt1 < len(keys1) || pt2 < len(keys2) {
		if pt1 == len(keys1) {
			keys[pt] = keys2[pt2]
			values[pt] = values2[pt2]
			pt2 += 1
		} else if pt2 == len(keys2) {
			keys[pt] = keys1[pt1]
			values[pt] = values1[pt1]
			pt1 += 1
		} else {
			compare_res := bytes.Compare(keys1[pt1], keys2[pt2])
			if compare_res <= 0 {
				keys[pt] = keys1[pt1]
				values[pt] = values1[pt1]
				pt1 += 1
			} else {
				keys[pt] = keys2[pt2]
				values[pt] = values2[pt2]
				pt2 += 1
			}
		}
		pt += 1
	}
	return keys, values
}

func sort(keys [][]byte, values [][]byte) ([][]byte, [][]byte) {
	if keys == nil || len(keys) <= 1 {
		return keys, values
	} else {
		keys1, values1 := sort(keys[:len(keys)/2], values[:len(values)/2])
		keys2, values2 := sort(keys[len(keys)/2:], values[len(values)/2:])
		sorted_keys, sorted_values := merge(keys1, values1, keys2, values2)
		return sorted_keys, sorted_values
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])

	f, err := os.Open(os.Args[1])
	check(err)
	defer f.Close()
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for err == nil {
		key := make([]byte, 10)
		value := make([]byte, 90)
		// 1. read bytes from the inputfile
		_, err = f.Read(key)
		if err != nil {
			break
		}
		_, err = f.Read(value)
		if err != nil {
			break
		}
		// 2. append key and value to data structure
		keys = append(keys, key)
		values = append(values, value)
	}

	// 3. sort keys and values
	sorted_keys, sorted_values := sort(keys, values)
	fmt.Println("length:", len(sorted_keys))

	// 4. write the result to outputfile
	os.Remove(os.Args[2])
	ff, err := os.Create(os.Args[2])
	check(err)
	defer ff.Close()
	for i, _ := range sorted_keys {
		sorted_key := sorted_keys[i]
		sorted_value := sorted_values[i]
		_, err = ff.Write(sorted_key)
		check(err)
		_, err = ff.Write(sorted_value)
		check(err)
	}
}
