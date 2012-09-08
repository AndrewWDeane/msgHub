// msgHub - serve TCP port, accept subcriptions and route publications onto required clients

// JSON message format:
// {"event": "sub",	"type": "messageType",	"key": "messageKey",		"id": "optional id. used for same sub down same tcp client"}
// {"event": "unsub",	"type": "messageType",	"key": "messageKey",		"id": "optional id. used for same sub down same tcp client"}
// {"event": "pub",	"type": "messageType",	"key": "messageKey",		........ any json data}

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"ad/msgHub/enc"
	"ad/msgHub/sock"
)

var version = "1.1.0"
var logger *log.Logger = log.New(os.Stdout, "", log.Ldate+log.Lmicroseconds)
var msgMap map[string]interface{}

type Subscription struct {
	RespCh chan []byte
}

func main() {

	sock.Logger = logger
	runtime.GOMAXPROCS(runtime.NumCPU())
	logger.Println("msgHub", version)

	df := flag.Int("tcpDelimiter", 10, "TCP message delimiter")
	tf := flag.Int("tcpPort", 0, "TCP port")

	flag.Parse()

	tcpDelimiter := enc.EncByte(*df)
	tcpPort := *tf

	hostname, _ := os.Hostname()
	host, _ := net.LookupHost(hostname)
	logger.Println("Serving on:", host, tcpPort)

	tcp := make(chan sock.ByteMessage, sock.ChannelBuffer)
	go sock.ServeTCPChannel(fmt.Sprintf("%v:%v", host, tcpPort), tcpDelimiter, tcp)

	storage := make(map[string]map[string]map[string]Subscription)

	for {
		select {
		case msg := <-tcp:
			msgMap = nil
			if err := json.Unmarshal(msg.Msg, &msgMap); err == nil {
				logger.Println("tcp:", msg.RemoteAddr, msgMap)

				if typeValue, keyValue, idValue, e := initialParse(); e == nil {
					event := msgMap["event"]
					switch event {
					case "pub":
						if msgType := storage[typeValue]; msgType != nil {
							if key := msgType[keyValue]; key != nil {
								for _, sub := range key {
									// start goroutines so one blocking client doesn't stop all
									go func(c chan []byte, m []byte) {
										c <- m
									}(sub.RespCh, msg.Msg)
								}
							}
						}

					case "sub", "unsub":
						// append the messages remote address to the id for uniqueness
						idValue += fmt.Sprintf("|%v", msg.RemoteAddr.String())

						msgType := storage[typeValue]
						if msgType == nil {
							msgType = make(map[string]map[string]Subscription)
						}

						key := msgType[keyValue]
						if key == nil {
							key = make(map[string]Subscription)
						}

						if event == "sub" {
							key[idValue] = Subscription{RespCh: msg.RespCh}
						} else {
							delete(key, idValue)
						}

						msgType[keyValue] = key
						storage[typeValue] = msgType
						//logger.Printf("%v for [%v][%v][%v]", event, typeValue, keyValue, idValue)

					case "admin":
						// do admin
					}
				} else {
					logger.Println(e)
				}
			}
		}
	}

}

func initialParse() (t string, k string, i string, e error) {
	defer func() {
		if i := recover(); i != nil {
			e = errors.New(fmt.Sprintf("%v", i))
		}
	}()

	t = msgMap["type"].(string)
	k = msgMap["key"].(string)

	// id is optional, so deal with it on its own
	var err error
	i, err = parseID()
	if err != nil {
		i = ""
	}

	return
}

func parseID() (i string, e error) {
	defer func() {
		if i := recover(); i != nil {
			e = errors.New(fmt.Sprintf("%v", i))
		}
	}()

	i = msgMap["id"].(string)
	return
}
