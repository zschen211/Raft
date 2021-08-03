package main

import (
	"bufio"
	"fmt"
	"mp3/util"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var branchId string

var mu sync.Mutex
var servers map[string]net.Conn
var servInfos map[string]util.ServInfo

var wg sync.WaitGroup

// if opType is CO, the message is stored in server field
var cmdChan map[string]chan *txCmd // { TxId: chan *txCmd }
var commit2PChan map[string]chan bool

var omu sync.Mutex
var cond *sync.Cond

// Server states
var objState map[string]*TsoState // { Account: TsoState }

type txCmd struct {
	txId    string
	opType  string
	server  string
	account string
	amount  int
}

func (t txCmd) String() string {
	str := "TX " + t.opType
	switch t.opType {
	case "CO":
		str = str + " " + t.server
	case "WITHDRAW", "DEPOSIT":
		str = fmt.Sprintf("%s %s %s %d %s", str, t.server, t.account, t.amount, t.txId)
	case "BALANCE":
		str = fmt.Sprintf("%s %s %s %s", str, t.server, t.account, t.txId)
	default:
		str = str + " " + t.txId
	}
	return str
}

func parseTx(split []string) *txCmd {
	t := &txCmd{
		txId:   split[len(split)-1],
		opType: split[1],
	}
	if t.opType == "CO" {
		t.server = split[2]
		t.txId = split[3]
	} else if t.opType == "BEGIN" {

	} else if t.opType != "ABORT" && t.opType != "COMMIT" {
		// BALANCE, DEPOSIT, WITHDRAW
		t.server = split[2]
		t.account = split[3]
		if t.opType != "BALANCE" {
			var err error
			t.amount, err = strconv.Atoi(split[4])
			util.HandleErr(err, "Unrecognized withdraw amount", split[4])
		}
	}
	return t
}

// Timestamp ordering write rule
// Return TRUE if added tentative write, FALSE if abort
func writeRule(account string, amount int, txId string) bool {
	if _, ok := objState[account]; !ok {
		objState[account] = InitState()
	}
	object := objState[account]
	object.mu.Lock()
	defer object.mu.Unlock()

	var maxRead string
	if len(*object.Rts) != 0 {
		maxRead = (*object.Rts)[len(*object.Rts)-1]
	}
	if txId >= maxRead && txId > object.CommitVersion {
		for k := range object.Tw {
			// update entry if existed
			if k == txId {
				object.Tw[k] = amount
				return true
			}
		}
		// Add new entry to tw otherwise
		AddToTw(object.Tw, amount, txId)
		return true
	} else {
		return false
	}
}

// Timestamp ordering read rule
// Return (val,TRUE) if read proceeds, (-1,FALSE) if not exist, (0,FALSE) if abort
func readRule(account string, txId string) (int, bool) {
reapply:
	if _, ok := objState[account]; !ok {
		// account not created yet
		return -1, false
	}
	object := objState[account]
	object.mu.Lock()

	if txId > object.CommitVersion {
		//search across the committed timestamp and the TW list for object D
		var version string
		var readVal int
		if object.CommitVersion > version && object.CommitVersion <= txId {
			version = object.CommitVersion
		}
		keys := getTwKeys(account)
		for _, k := range keys {
			if k <= txId && k > version {
				version = k
			}
		}

		if version == object.CommitVersion {
			if version == "" {
				object.mu.Unlock()
				return -1, false
			} else {
				AddToRts(object.Rts, txId)
				readVal = object.CommitVal
				object.mu.Unlock()
				return readVal, true
			}
		} else {
			if version == txId {
				readVal = object.Tw[version]
				object.mu.Unlock()
				return readVal, true
			} else {
				// wait unitl version is committed or aborted
				object.mu.Unlock()
				cond.L.Lock()
				aborted := false
				committed := false
				for aborted == false && committed == false {
					cond.Wait()
					_, ok := object.Tw[version]
					committed = version == object.CommitVersion
					aborted = !ok
				}
				cond.L.Unlock()
				goto reapply
			}
		}
	} else {
		object.mu.Unlock()
		return 0, false
	}
}

// Timestamp ordering commit
func commit(txId string) {
	commitObjs := make(map[string]*TsoState)
	for account, object := range objState {
		object.mu.Lock()
		if _, ok := object.Tw[txId]; ok {
			commitObjs[account] = object
		}
		object.mu.Unlock()
	}
	ready := commitReady(commitObjs, txId)
	cond.L.Lock()
	for ready == false {
		cond.Wait()
		ready = commitReady(commitObjs, txId)
	}
	cond.L.Unlock()
	if ready == true {
		for _, obj := range commitObjs {
			obj.mu.Lock()
			obj.CommitVal = obj.Tw[txId]
			obj.CommitVersion = txId
			delete(obj.Tw, txId)
			obj.mu.Unlock()
		}
	}
	fmt.Print("BALANCES")
	for acc, obj := range objState {
		obj.mu.Lock()
		if obj.CommitVal != 0 {
			fmt.Printf(" %s:%d", acc, obj.CommitVal)
		}
		obj.mu.Unlock()
	}
	fmt.Println()
	cond.L.Lock()
	cond.Broadcast()
	cond.L.Unlock()
}

// Timestamp ordering abort
func abort(txId string) {
	for name, object := range objState {
		object.mu.Lock()
		if _, ok := object.Tw[txId]; ok {
			delete(object.Tw, txId)
		}
		object.mu.Unlock()
		if len(object.Tw) == 0 && object.CommitVersion == "" {
			delete(objState, name)
		}
	}
	cond.L.Lock()
	cond.Broadcast()
	cond.L.Unlock()
}

func handleTx(conn net.Conn, txId string) {
	ch := cmdChan[txId]
	defer delete(cmdChan, txId)
	defer close(ch)
	// defer println("Exit", txId)
	txBranch := make(map[string]bool)
	for {
		cmd := <-ch
		// println("cmd:", cmd.String())
		if cmd.opType == "END" {
			return
		}
		switch cmd.opType {
		case "DEPOSIT", "WITHDRAW", "BALANCE":
			if cmd.server != branchId {
				_, err := fmt.Fprintln(servers[cmd.server], cmd)
				util.HandleErr(err, "Server disconnected", cmd.server)
				if cmd.opType != "BALANCE" {
					txBranch[cmd.server] = true
				}
				continue
			}
		}
		switch cmd.opType {
		case "WITHDRAW", "BALANCE":
			// Check existence
			if _, exist := objState[cmd.account]; !exist {
				// println("NE")
				go abort2P(txBranch, cmd.txId, nil)
				util.SendMessageLn(conn, "", "TX CO NE "+txId)
				return
			}
		}
		switch cmd.opType {
		case "DEPOSIT":
			var ok bool
			var curBalance int
			if curBalance, ok = readRule(cmd.account, cmd.txId); ok {
				ok = writeRule(cmd.account, curBalance+cmd.amount, cmd.txId)
			} else if curBalance == -1 {
				ok = writeRule(cmd.account, cmd.amount, cmd.txId)
			}
			flag := "OK"
			if !ok {
				flag = "ABORT"
				go abort2P(txBranch, cmd.txId, conn)
				return
			}
			// println("Deposit", flag)
			_, err := fmt.Fprintln(conn, "TX CO", flag, cmd.txId)
			util.HandleErr(err, "Connection fail at reply for DEPOSIT")
		case "WITHDRAW":
			var ok bool
			var curBalance int
			if curBalance, ok = readRule(cmd.account, cmd.txId); ok {
				ok = writeRule(cmd.account, curBalance-cmd.amount, cmd.txId)
			}
			flag := "OK"
			if !ok {
				flag = "ABORT"
				go abort2P(txBranch, cmd.txId, conn)
				return
			}
			_, err := fmt.Fprintln(conn, "TX CO", flag, cmd.txId)
			util.HandleErr(err, "Connection fail at reply for WITHDRAW")
		case "BALANCE":
			if curBalance, ok := readRule(cmd.account, cmd.txId); ok {
				_, err := fmt.Fprintln(conn, "TX CO", curBalance, cmd.txId)
				util.HandleErr(err, "Connection fail at reply current balance")
			} else {
				// println("Read rule fail")
				go abort2P(txBranch, cmd.txId, conn)
				return
			}
		case "ABORT":
			go abort2P(txBranch, cmd.txId, conn)
			return
		case "COMMIT":
			go commit2P(txBranch, cmd.txId, conn)
			return
		case "CO":
			switch cmd.server {
			case "ABORT":
				go abort2P(txBranch, cmd.txId, conn)
				return
			case "NE":
				go abort2P(txBranch, cmd.txId, nil)
				util.SendMessageLn(conn, "", cmd)
				return
			}
			_, err := fmt.Fprintln(conn, cmd)
			util.HandleErr(err, "Connection fail at reply CO")
		default:
			// println("Uncaught message", cmd)
		}
	}
}

func handleReceive(conn net.Conn) {
	reader := bufio.NewReader(conn)
	name, err := reader.ReadString('\n')
	if err != nil {
		println("Connection failed")
	}
	name = strings.TrimSpace(name)
	println(name, "connected")
	_, rec := servers[name]
	if _, e := servInfos[name]; !rec && e {
		reconnect(name)
	}
	var sendConn net.Conn
	var exist bool
	if sendConn, exist = servers[name]; !exist {
		sendConn = conn
	}
	for {
		msg, err := reader.ReadString('\n')
		msg = strings.TrimSpace(msg)
		if err != nil {
			println(name, "disconnected.")
			mu.Lock()
			delete(servers, name)
			mu.Unlock()
			return
		}
		// Parse message
		// println("msg:", msg)
		msgSplit := strings.Split(msg, " ")
		switch msgSplit[0] {
		case "TX": // tx message received
			cmd := parseTx(msgSplit)
			if bCh, exists := cmdChan[cmd.txId]; exists {
				// Transaction channel existed
				bCh <- cmd
			} else {
				// txId first received
				// println("First time receive")
				ch := make(chan *txCmd)
				cmdChan[cmd.txId] = ch
				go handleTx(sendConn, cmd.txId)
				ch <- cmd
			}
		case "2P": // 2p commit message received
			txId := msgSplit[2]
			switch opType := msgSplit[1]; opType {
			case "PREP":
				reply := fmt.Sprintf("2P REP %s ", txId)
				if consistencyCheck(msgSplit[2]) {
					reply += "YES"
				} else {
					reply += "NO"
				}
				util.SendMessageLn(sendConn, "Connection fail at reply.", reply)
			case "REP":
				if msgSplit[3] == "YES" {
					commit2PChan[txId] <- false
				} else {
					commit2PChan[txId] <- true
				}
			case "FNL":
				reply := fmt.Sprintf("2P ACK %s", txId)
				switch msgSplit[3] {
				case "COMMIT":
					commit(txId)
				case "ABORT":
					go abort(txId)
				}
				util.SendMessageLn(sendConn, "Connection fail at reply FNL.", reply)
				if ch, exist := cmdChan[txId]; exist {
					ch <- &txCmd{
						opType: "END",
					}
				}
			case "ACK":
				if _, exist := commit2PChan[txId]; exist {
					commit2PChan[txId] <- true
				}
			}
		}
	}
}

// Check consistency
func consistencyCheck(txId string) bool {
	for _, obj := range objState {
		if val, exist := obj.Tw[txId]; exist {
			if val < 0 {
				return false
			}
		}
	}
	return true
}

func abort2P(branches map[string]bool, txId string, conn net.Conn) {
	go abort(txId)
	for branch := range branches {
		util.SendMessageF(servers[branch], "Connection fail with "+branch, "2P FNL %s ABORT\n", txId)
	}
	if conn != nil {
		util.SendMessageLn(conn, "Connection fail at sending abort", "TX CO ABORT "+txId)
	}
}

// 2P commit, start the commit process for all relevant servers
func commit2P(branches map[string]bool, txId string, clientConn net.Conn) {
	if !consistencyCheck(txId) {
		go abort2P(branches, txId, clientConn)
		return
	}
	var ch chan bool
	var exist bool
	if ch, exist = commit2PChan[txId]; !exist {
		ch = make(chan bool)
		commit2PChan[txId] = ch
	}
	for branch := range branches {
		_, err := fmt.Fprintln(servers[branch], "2P PREP", txId)
		util.HandleErr(err, "Connection fail with", branch)
	}
	flag := "COMMIT"
	for range branches {
		if <-ch {
			flag = "ABORT"
		}
	}
	// println(flag)
	for branch := range branches {
		_, err := fmt.Fprintf(servers[branch], "2P FNL %s %s\n", txId, flag)
		util.HandleErr(err, "Connection fail with", branch)
	}
	if flag == "COMMIT" {
		commit(txId)
	} else {
		go abort(txId)
	}
	for range branches {
		<-ch
	}
	close(commit2PChan[txId])
	delete(commit2PChan, txId)
	util.SendMessageF(clientConn, "client offline", "TX CO %s %s\n", flag, txId)
}

// Make connections to servers exclusively
func connectServ(branch string, ser util.ServInfo) {
	defer wg.Done()
	connInfo := strings.Join([]string{ser.ServAddr, ser.ServPort}, ":")
	conn, err := net.Dial("tcp", connInfo)
	for err != nil {
		conn, err = net.Dial("tcp", connInfo)
	}
	mu.Lock()
	servers[branch] = conn
	mu.Unlock()
	_, err = fmt.Fprintln(conn, branchId)
	util.HandleErr(err, "Connection fail")
}

func reconnect(branch string) {
	ser := servInfos[branch]
	connInfo := strings.Join([]string{ser.ServAddr, ser.ServPort}, ":")
	conn, err := net.Dial("tcp", connInfo)
	for err != nil {
		conn, err = net.Dial("tcp", connInfo)
	}
	mu.Lock()
	servers[branch] = conn
	mu.Unlock()
	util.SendMessageLn(conn, "Connection fail with "+branchId, branchId)
	println(branch, "reconnected")
}

func main() {
	cond = sync.NewCond(&omu)
	objState = make(map[string]*TsoState)
	servers = make(map[string]net.Conn)
	commit2PChan = make(map[string]chan bool)
	argv := os.Args[1:]
	if len(argv) != 2 {
		println("usage: ./server <BRANCH_ID> <CONFIG>\n")
		os.Exit(1)
	}
	branchId = argv[0]
	configFile := argv[1]
	servInfos = util.ParseConfig(configFile)
	cmdChan = make(map[string]chan *txCmd)
	println(" ")
	ln, err := net.Listen("tcp", servInfos[branchId].ServAddr+":"+servInfos[branchId].ServPort)
	util.HandleErr(err, "Connection error")
	println(branchId, "starts at port", servInfos[branchId].ServPort)

	for b, s := range servInfos {
		if b != branchId {
			wg.Add(1)
			go connectServ(b, s)
		}
	}
	wg.Wait()
	println("Server ready to go.")
	for {
		conn, err := ln.Accept()
		util.HandleErr(err)
		go handleReceive(conn)
	}
}
