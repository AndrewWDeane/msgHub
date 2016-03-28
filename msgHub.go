// msgHub - serve TCP port, accept subcriptions and route publications onto required clients

// JSON message format:
// {"event": "sub",	"type": "messageType. CSV",	"key": "messageKey. CSV",		"subkey": "optional string. may be wildcard", 	"id": "optional id. used for same sub down same tcp client" "echoFields": "optional any JSON to be echo back to the client"}
// {"event": "unsub",	"type": "messageType. CSV",	"key": "messageKey. CSV",		"subkey": "optional string. may be wildcard", 	"id": "optional id. used for same sub down same tcp client"}
// {"event": "pub",	"type": "messageType",	"key": "messageKey",			"subkey": "optional string", 	........ any json data}

// {"event": "admin", "printStack": "yes", "type": "", "key": ""}

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
	"strings"
	"time"

	"enc"
	"sock"
)

var version = "1.10.10"
var logger *log.Logger = log.New(os.Stdout, "", log.Ldate+log.Lmicroseconds)
var msgMap map[string]interface{}

type Data struct {
	Batch []map[string]interface{}
}

type Subscription struct {
	RespCh     chan []byte
	EchoFields interface{}
}

func main() {

	sock.Logger = logger
	runtime.GOMAXPROCS(runtime.NumCPU())
	logger.Println("msgHub", version)

	df := flag.Int("tcpDelimiter", 10, "TCP message delimiter")
	tf := flag.Int("tcpPort", 0, "TCP port")
	tbsf := flag.Int("tcpBatchSize", 1, "TCP output max batch size")
	ttf := flag.String("tcpBatchTimeout", "1s", "TCP batch timeout. Set tcpBatchSize > 1 to take effect.")
	bf := flag.Bool("subBatchOutput", true, "Batch subscription output")
	bsf := flag.Int("subBatchSize", 1024, "Subscription output batch size")
	btf := flag.String("subBatchTimeout", "1s", "Subscription batch timeout")
	bbf := flag.Int("subBatchingBuffer", 1024, "Subscription batch buffer size")
	qf := flag.Bool("quiet", false, "Turn off log output")

	flag.Parse()

	tcpDelimiter := enc.EncByte(*df)
	tcpPort := *tf
	batching := *bf
	batchBuffer := *bbf
	batchSize := *bsf
	quiet := *qf
	sock.WriteBatchSize = *tbsf

	var batchingTimeOut time.Duration
	var err error

	if batchingTimeOut, err = time.ParseDuration(*btf); err != nil {
		batchingTimeOut, _ = time.ParseDuration("1s")
	}

	if sock.WriteBatchSize > 1 {
		var tcpTimeOut time.Duration
		if tcpTimeOut, err = time.ParseDuration(*ttf); err == nil {
			sock.WriteTimeout = tcpTimeOut
		}
	}

	hostname, _ := os.Hostname()
	host, _ := net.LookupHost(hostname)
	logger.Println("Serving on:", host, tcpPort)

	if batching {
		logger.Printf("Subscription batching: size %v timeout %v buffer %v\n", batchSize, batchingTimeOut, batchBuffer)
	}

	if sock.WriteBatchSize > 1 {
		logger.Printf("TCP batching: size %v timeout %v \n", sock.WriteBatchSize, sock.WriteTimeout)
	}

	tcp := make(chan sock.ByteMessage, sock.ChannelBuffer)
	go sock.ServeTCPChannel(fmt.Sprintf("%v:%v", host, tcpPort), tcpDelimiter, tcp)

	storage := make(map[string]map[string]map[string]map[string]Subscription)

	for {
		select {
		case msg := <-tcp:
			if msg.Disconnect {
				// disconnected - tidy up any subs
				for _, t := range storage {
					for _, k := range t {
						for _, sk := range k {
							for id, s := range sk {
								a := strings.Split(id, "|")
								if a[1] == msg.RemoteAddr.String() {
									if batching {
										s.RespCh <- nil
									}
									delete(sk, id)
								}

							}
						}
					}
				}
			} else {
				msgMap = nil
				if err := json.Unmarshal(msg.Msg, &msgMap); err == nil {

					if !quiet {
						logger.Println("tcp:", msg.RemoteAddr, msgMap)
					}

					if typeValue, keyValue, subkeyValue, idValue, e := initialParse(); e == nil {
						event := msgMap["event"]
						switch event {
						case "pub":
							if msgType := storage[typeValue]; msgType != nil {
								if key := msgType[keyValue]; key != nil {
									if subkey := key[subkeyValue]; subkey != nil {
										send(msg, subkey)
									}
									if subkey := key["*"]; subkey != nil {
										send(msg, subkey)
									}
								}
							}

						case "sub", "unsub":
							// append the messages remote address to the id for uniqueness
							idValue += fmt.Sprintf("|%v", msg.RemoteAddr.String())

							typeValues := strings.Split(typeValue, ",")

							for _, typeValue = range typeValues {

								msgType := storage[typeValue]
								if msgType == nil {
									msgType = make(map[string]map[string]map[string]Subscription)
								}

								keyValues := strings.Split(keyValue, ",")

								for _, keyValue = range keyValues {

									key := msgType[keyValue]
									if key == nil {
										key = make(map[string]map[string]Subscription)
									}

									subkey := key[subkeyValue]
									if subkey == nil {
										subkey = make(map[string]Subscription)
									}

									if event == "sub" {
										sub := Subscription{RespCh: msg.RespCh, EchoFields: msgMap["echoFields"]}

										if batching {
											batcher := make(chan []byte, batchBuffer)
											go func(toTCP, toBatcher chan []byte) {

												batch := Data{Batch: make([]map[string]interface{}, batchSize)}
												pos := 0
												timeout := time.After(batchingTimeOut)
												for {
													select {
													case m := <-toBatcher:
														if m == nil {
															return
														}

														// Just how expensive is this back and forth between bytes and JSON ???
														_ = json.Unmarshal(m, &batch.Batch[pos])
														pos += 1
														if pos == batchSize {
															// flush - all
															b, _ := json.Marshal(batch)
															toTCP <- b
															batch.Batch = make([]map[string]interface{}, batchSize)
															pos = 0
														}

													case _ = <-timeout:
														if batch.Batch[0] != nil {
															//flush - up to the populated element by slicing
															batch.Batch = batch.Batch[0:pos]
															b, _ := json.Marshal(batch)
															toTCP <- b
															batch.Batch = make([]map[string]interface{}, batchSize)
															pos = 0
														}
														timeout = time.After(batchingTimeOut)
													}
												}

											}(sub.RespCh, batcher)

											sub.RespCh = batcher
										}

										subkey[idValue] = sub

									} else {
										if sub, ok := subkey[idValue]; ok {
											if batching {
												sub.RespCh <- nil
											}
											delete(subkey, idValue)
										}
									}

									key[subkeyValue] = subkey
									msgType[keyValue] = key
									storage[typeValue] = msgType
								}
							}
						case "admin":
							// do admin
							if msgMap["printStack"] == "yes" {
								PrintStack(true)
							}
						}
					} else {
						logger.Println(e)
					}
				}
			}
		}
	}

}

//Print current stack trace
func PrintStack(all bool) {
	buffer := make([]byte, 10240)
	numB := runtime.Stack(buffer, all)
	logger.Printf("%v current goroutines\n", runtime.NumGoroutine())
	if numB > 10240 {
		numB = 10240
	}
	logger.Println(string(buffer[0:numB]))
}

func send(msg sock.ByteMessage, subkey map[string]Subscription) {
	for _, sub := range subkey {

		output := msg.Msg

		if sub.EchoFields != nil {
			// careful now; msgMap is a global
			msgMap["echoFields"] = sub.EchoFields
			output, _ = json.Marshal(msgMap)
			delete(msgMap, "echoFields")
		}

		// start goroutines so one blocking client doesn't stop all
		go func(c chan []byte, m []byte) {
			c <- m
		}(sub.RespCh, output)
	}

}

func initialParse() (t, k, s, i string, e error) {
	defer func() {
		if i := recover(); i != nil {
			e = errors.New(fmt.Sprintf("%v", i))
		}
	}()

	t = msgMap["type"].(string)
	k = msgMap["key"].(string)

	// optional fields, so deal with them on their own
	var err error
	i, err = parseExtra("id")
	if err != nil {
		i = ""
	}
	s, err = parseExtra("subkey")
	if err != nil {
		s = ""
	}

	return
}

func parseExtra(key string) (i string, e error) {
	defer func() {
		if i := recover(); i != nil {
			e = errors.New(fmt.Sprintf("%v", i))
		}
	}()

	i = msgMap[key].(string)
	return
}
