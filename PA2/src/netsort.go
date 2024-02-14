package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := os.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)
	if err != nil {
		log.Fatal(err)
	}

	return scs
}

// golang server
func listen(clientServerWG *sync.WaitGroup, Host string, Port string, amount int, tempFile string) {
	address := Host + ":" + Port
	ln, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	// initialize mutex with value of (amount-1)
	wg.Add(amount - 1)

	// create threads to handle (amount-1) connections
	for i := 0; i < amount-1; i += 1 {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// sub-thread for listener connection
		go handleConnection(conn, i, &wg, tempFile)
	}
	wg.Wait()
	clientServerWG.Done()
}

// thread handler to deal with detailed work
func handleConnection(conn net.Conn, conn_id int, wg *sync.WaitGroup, tempFile string) {
	tempF, err := os.OpenFile(tempFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	buffer := make([]byte, 100)
	for {
		_, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		key := buffer[:10]
		value := buffer[10:]
		tempF.Write(key)
		tempF.Write(value)
	}
	conn.Close()
	tempF.Close()
	wg.Done()
}

// golang client
func dial(clientServerWG *sync.WaitGroup, scs ServerConfigs, serverId int, sendKeys map[int][][]byte, sendValues map[int][][]byte) {
	for i := 0; i < len(scs.Servers); i++ {
		if i == serverId {
			continue
		}
		address := scs.Servers[i].Host + ":" + scs.Servers[i].Port
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		for j := 0; j < len(sendKeys[i]); j += 1 {
			key := sendKeys[i][j]
			value := sendValues[i][j]
			keyValuePair := make([]byte, 0)
			keyValuePair = append(keyValuePair, key...)
			keyValuePair = append(keyValuePair, value...)
			conn.Write(keyValuePair)
		}
	}
	clientServerWG.Done()
}

func getIndex(key []byte, amount int) int {
	numIdxs := int(math.Log2(float64(amount)))
	index := 0
	for i := 0; i < numIdxs; i += 1 {
		currByteLoc := i / 8
		currByte := key[currByteLoc]
		mask := (uint8(1) << (7 - i%8))
		if (mask & currByte) != 0 {
			index = index*2 + 1
		} else {
			index = index * 2
		}
	}
	return index
}

func partition(keys [][]byte, values [][]byte, amount int) (map[int][][]byte, map[int][][]byte) {
	res_keys := make(map[int][][]byte)
	res_values := make(map[int][][]byte)
	for i := 0; i < amount; i += 1 {
		res_keys[i] = make([][]byte, 0)
		res_values[i] = make([][]byte, 0)
	}

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		value := values[i]
		index := getIndex(key, amount)
		res_keys[index] = append(res_keys[index], key)
		res_values[index] = append(res_values[index], value)
	}
	return res_keys, res_values
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

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	amount := len(scs.Servers)
	fmt.Println("my host name:", scs.Servers[serverId].Host)

	// read the input file
	f, err := os.Open(os.Args[2])
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for {
		key := make([]byte, 10)
		value := make([]byte, 90)
		_, err = f.Read(key)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		_, err = f.Read(value)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		keys = append(keys, key)
		values = append(values, value)
	}

	// partition the data
	sendKeys, sendValues := partition(keys, values, amount)
	tempFile := "temp_file.dat"
	os.Remove(tempFile)
	// append the data
	tempF, err := os.OpenFile(tempFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// store server's own data in the temp file
	for i := 0; i < len(sendKeys[serverId]); i += 1 {
		key := sendKeys[serverId][i]
		value := sendValues[serverId][i]
		tempF.Write(key)
		tempF.Write(value)
	}
	tempF.Close()

	// create a client, a server, and a mutex to exchange data, then store the data in temp file
	var clientServerWG sync.WaitGroup
	clientServerWG.Add(2)
	go listen(&clientServerWG, scs.Servers[serverId].Host, scs.Servers[serverId].Port, amount, tempFile)
	time.Sleep(2 * time.Second)
	go dial(&clientServerWG, scs, serverId, sendKeys, sendValues)
	clientServerWG.Wait()

	// retreive the data from temp file
	tempF, err = os.Open(tempFile)
	if err != nil {
		log.Fatal(err)
	}

	receive_keys := make([][]byte, 0)
	receive_values := make([][]byte, 0)

	for {
		key := make([]byte, 10)
		value := make([]byte, 90)
		_, err = tempF.Read(key)
		if err != nil {
			break
		}
		_, err = tempF.Read(value)
		if err != nil {
			break
		}
		receive_keys = append(receive_keys, key)
		receive_values = append(receive_values, value)
	}
	tempF.Close()
	fmt.Println("number of data in the server", len(receive_keys))
	os.Remove(tempFile)

	// sort the data
	sorted_keys, sorted_values := sort(receive_keys, receive_values)
	// write the sorted data to outputfile
	os.Remove(os.Args[3])
	outputFile, err := os.OpenFile(os.Args[3], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(sorted_keys); i += 1 {
		key := sorted_keys[i]
		value := sorted_values[i]
		outputFile.Write(key)
		outputFile.Write(value)
	}
	outputFile.Close()
}
