package util

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type ServInfo struct {
	ServAddr string
	ServPort string
}

type TwoPhaseMsg struct {
	MsgType string
}

type TxCmd struct {
	Header  string
	Branch  string
	Account string
	Amount  int
}

// Helper for handling any error
func HandleErr(err error, msg ...interface{}) {
	if err != nil {
		println(msg)
		panic(err)
	}
}

func SendMessageF(conn net.Conn, msg string, format string, a ...interface{}) {
	_, err := fmt.Fprintf(conn, format, a...)
	HandleErr(err, msg)
}

func SendMessageLn(conn net.Conn, msg string, a ...interface{}) {
	_, err := fmt.Fprintln(conn, a...)
	HandleErr(err, msg)
}

// Parse config file and return a map (key: branch; value: ServInfo struct)
func ParseConfig(configFile string) map[string]ServInfo {
	f, err := os.Open(configFile)
	HandleErr(err)
	servInfos := make(map[string]ServInfo)
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		infoList := strings.Split(scanner.Text(), " ")
		servInfos[infoList[0]] = ServInfo{ServAddr: infoList[1], ServPort: infoList[2]}
	}
	HandleErr(f.Close(), "IMPOSSIBLE!")
	return servInfos
}

// Parse transaction commands from stdin
func ParseTx(cmd string) *TxCmd {
	transaction := &TxCmd{}
	cmdSplits := strings.Split(cmd, " ")
	if len(cmdSplits) == 1 {
		// BEGIN, COMMIT, and ABORT
		transaction.Header = strings.TrimSpace(cmdSplits[0])
	} else if len(cmdSplits) == 2 {
		// BALANCE
		transaction.Header = strings.TrimSpace(cmdSplits[0])
		transaction.Branch = strings.Split(cmdSplits[1], ".")[0]
		transaction.Account = strings.TrimSpace(strings.Split(cmdSplits[1], ".")[1])
	} else if len(cmdSplits) == 3 {
		// DEPOSIT and WITHDRAW
		transaction.Header = strings.TrimSpace(cmdSplits[0])
		transaction.Branch = strings.Split(cmdSplits[1], ".")[0]
		transaction.Account = strings.TrimSpace(strings.Split(cmdSplits[1], ".")[1])
		var err error
		transaction.Amount, err = strconv.Atoi(strings.TrimSpace(cmdSplits[2]))
		HandleErr(err, "Invalid transaction amount")
	}
	return transaction
}
