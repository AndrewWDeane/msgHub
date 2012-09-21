msgHub
======

Route JSON based messages over TCP

run hub up using:

msgHub -tcpPort=[port] -tcpDelimiter=[ascii code]

e.g. start on 12345 using line feed as delimiter

msgHub -tcpPort=12345 -tcpDelimiter=10

JSON message format:

{"event": "sub",	"type": "messageType. CSV",	"key": "messageKey. CSV",		"subkey": "optional. may be wildcard",   "id": "optional id. used for same sub down same tcp client" "echoFields": "optional. JSON will be echoed back to the client with any message"}

{"event": "unsub",	"type": "messageType. CSV",	"key": "messageKey. CSV",		"subkey": "optional. may be wildcard",   "id": "optional id. used for same sub down same tcp client"}

{"event": "pub",	"type": "messageType",	"key": "messageKey",		"subkey": "optional",   ........ any json data}

Built file is linux64

Usage of ./msgHub:
  -quiet=false:             Turn off log output
  -subBatchOutput=true:     Batch subscription output
  -subBatchSize=1024:       Subscription output batch size
  -subBatchTimeout="1s":    Subscription batch timeout
  -subBatchingBuffer=1024:  Subscription batch buffer size
  -tcpBatchSize=1:          TCP output max batch size
  -tcpBatchTimeout="1s":    TCP batch timeout. Set tcpBatchSize > 1 to take effect.
  -tcpDelimiter=10:         TCP message delimiter
  -tcpPort=0:               TCP port

