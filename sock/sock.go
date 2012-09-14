// sock - package contains standard byte sock utilities
package sock

import (
	"net"
	"errors"
	"log"
	"bytes"
)

var ChannelBuffer = 4096
var SocketBuffer = 4096
var Logger *log.Logger

// ByteMessage holds the raw socket bytes, the address of the remote tcp client, and the channel to reply on
type ByteMessage struct {
	Msg        []byte
	RemoteAddr net.Addr
	RespCh     chan []byte
	Disconnect bool
}

// Serve TCP socket at given address, Supply destination channel  for all socket messages.
// ByteMessage are placed on the clients channel, which contain the channel to respond to the socket on.
// Two seperate goroutines are spawned to handle the reading from and writing to the socket.
func ServeTCPChannel(addr string, delimiter []byte, toClient chan ByteMessage) error {
	a, err := net.ResolveTCPAddr("tcp4", addr)
	ln, err := net.ListenTCP("tcp4", a)

	if err != nil {
		Logger.Println(err)
		return err
	}

	for {
		if conn, err := ln.AcceptTCP(); err == nil {
			remote := conn.RemoteAddr()
			Logger.Println("Connection:", remote)

			go func() {
				toTCP := make(chan []byte, ChannelBuffer)
				fromTCP := make(chan ByteMessage, ChannelBuffer)

				go func() {
					for {
						if tcp, ok := <-fromTCP; !ok {
							return
						} else {
							tcp.RespCh = toTCP
							toClient <- tcp
						}
					}

				}()

				go ReadTCPChannel(conn, delimiter, fromTCP)
				WriteTCPChannel(conn, delimiter, toTCP)
				// if we get here then write has returned as the connection has dropped on write), so send msg toClient to make client tidy up
				toClient <- ByteMessage{RemoteAddr: remote, Disconnect: true}

			}()
		} else {
			Logger.Println(err)
			break
		}

	}

	return nil
}

// Read TCP socket and return complete ByteMessage to caller's channel
func ReadTCPChannel(conn *net.TCPConn, delimiter []byte, fromSocket chan ByteMessage) error {
	var message []byte
	buffer := make([]byte, SocketBuffer)

	for {
		if n, err := conn.Read(buffer); err != nil || n == 0 {
			if n == 0 && err == nil {
				err = errors.New("No bytes")
			}
			Logger.Println("Closing read:", conn.RemoteAddr(), err)
			conn.Close()
			return err
		} else {

			message = append(message, buffer[0:n]...)
			m := bytes.Split(message, delimiter)
			for i, entry := range m {
				if i < len(m)-1 {
					fromSocket <- ByteMessage{Msg: entry, RemoteAddr: conn.RemoteAddr()}
				} else {
					// overflow
					message = entry
				}
			}
		}

	}
	return nil
}

// Read bytes from caller's channel  and write to tcp socket
func WriteTCPChannel(conn *net.TCPConn, delimiter []byte, toSocket chan []byte) error {
	for {
		if message, ok := <-toSocket; !ok {
			err := errors.New("Error on socket channel")
			Logger.Println("Closing:", conn.RemoteAddr(), err)
			conn.Close()
			return err
		} else {
			message = append(message, delimiter...)
			if _, err := conn.Write(message); err != nil {
				Logger.Println("Closing write:", conn.RemoteAddr(), err)
				conn.Close()
				return err
			}
		}
	}
	return nil
}
