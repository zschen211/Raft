package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"mp3/util"
	"net"
	"os"
	"strings"
	"time"
)

const FileMode = false

var servNames = []string{"A", "B", "C", "D", "E"}
var servConns map[string]net.Conn
var clientId string
var txId string
var isInTx bool
var coordinator net.Conn
var counter int

// Client interface for handling incoming transactions
func handleTransaction() {
	reader := bufio.NewReader(os.Stdin)
	for {
		text, err := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if err != nil {
			os.Exit(1)
		}
		if FileMode {
			println(text)
		}
		cmd := util.ParseTx(text)
		if !isInTx && cmd.Header == "BEGIN" {
			connKey := servNames[rand.Intn(len(servNames))]
			coordinator = servConns[connKey]
			isInTx = true
			fmt.Println("OK")
			txId = fmt.Sprint(time.Now().UnixNano()/1000, clientId, counter)
			counter++
			if FileMode {
				println()
			}
			//fmt.Fprintf(coordinator, "TX BEGIN %s\n", clientId)
			continue
		}
		if isInTx {
			switch cmd.Header {
			case "COMMIT":
				util.SendMessageLn(coordinator, "", "TX COMMIT", txId)
			case "ABORT":
				util.SendMessageLn(coordinator, "", "TX ABORT", txId)
			case "BALANCE":
				util.SendMessageLn(coordinator, "", "TX BALANCE", cmd.Branch, cmd.Account, txId)
			case "DEPOSIT":
				util.SendMessageLn(coordinator, "", "TX DEPOSIT", cmd.Branch, cmd.Account, cmd.Amount, txId)
			case "WITHDRAW":
				util.SendMessageLn(coordinator, "", "TX WITHDRAW", cmd.Branch, cmd.Account, cmd.Amount, txId)
			default:
				// println("Unrecognized command", cmd.Header)
				continue
			}
			handleResponse(cmd)
		} else {
			// println("Enter 'BEGIN' to start transaction!")
		}
		if FileMode {
			println()
		}
	}
}

// Parse and handle the response msg from coordinator
// response format: 1.TX CO NE  2.TX CO ABORT  3.TX CO OK  4.TX CO %val%
func handleResponse(cmd *util.TxCmd) {
	reCh := make(chan string)
	go func(reCh chan string) {
		line, _ := bufio.NewReader(coordinator).ReadString('\n')
		reCh <- line
	}(reCh)
	response := <-reCh
	// print("Command:", response)
	switch msg := strings.TrimSpace(strings.Split(response, " ")[2]); msg {
	case "OK":
		fmt.Println("OK")
	case "NE":
		fmt.Println("NOT FOUND, ABORTED")
		isInTx = false
	case "ABORT":
		fmt.Println("ABORTED")
		isInTx = false
	case "COMMIT":
		fmt.Println("COMMIT OK")
		isInTx = false
	default:
		fmt.Printf("%s.%s = %s\n", cmd.Branch, cmd.Account, msg)
	}
}

func main() {
	servConns = make(map[string]net.Conn)
	argv := os.Args[1:]

	clientId = argv[0]
	configName := argv[1]

	// connect to all servConns (or branches)
	servInfos := util.ParseConfig(configName)
	for branch, s := range servInfos {
		conn, err := net.Dial("tcp", strings.Join([]string{s.ServAddr, s.ServPort}, ":"))
		util.HandleErr(err, "Server offline", branch)
		servConns[branch] = conn
		util.SendMessageLn(conn, "", clientId)
	}

	handleTransaction()
}
