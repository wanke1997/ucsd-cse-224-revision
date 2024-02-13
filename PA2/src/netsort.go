package main

import (
	"fmt"
	"log"
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
func listen(clientServerWG *sync.WaitGroup, Host string, Port string, amount int) {
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
		go handleConnection(conn, i, &wg)
	}
	wg.Wait()
	clientServerWG.Done()
}

// thread handler to deal with detailed work
func handleConnection(conn net.Conn, conn_id int, wg *sync.WaitGroup) {
	bs := make([]byte, 4)
	conn.Read(bs)
	str := string(bs)
	text := "This is connection with server " + str + ".\n"
	print(text)
	conn.Close()
	wg.Done()
}

// golang client
func dial(clientServerWG *sync.WaitGroup, scs ServerConfigs, serverId int) {
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
		bs := []byte(strconv.Itoa(serverId))
		conn.Write(bs)
	}
	clientServerWG.Done()
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

	// create a client, a server, and a mutex
	var clientServerWG sync.WaitGroup
	clientServerWG.Add(2)
	go listen(&clientServerWG, scs.Servers[serverId].Host, scs.Servers[serverId].Port, amount)
	time.Sleep(3 * time.Second)
	go dial(&clientServerWG, scs, serverId)
	clientServerWG.Wait()
}
