msgHub
======

Route JSON based messages over TCP

run hub up using:

msgHub -tcpPort=[port] -tcpDelimiter=[ascii code]

e.g. start on 12345 using line feed as delimiter

msgHub -tcpPort=12345 -tcpDelimiter=10

JSON message format:

{"event": "sub",	"type": "messageType",	"key": "messageKey",		"subkey": "optional. may be wildcard",   "id": "optional id. used for same sub down same tcp client" "echoFields": "optional. JSON will be echoed back to the client with any message"}

{"event": "unsub",	"type": "messageType",	"key": "messageKey",		"subkey": "optional. may be wildcard",   "id": "optional id. used for same sub down same tcp client"}

{"event": "pub",	"type": "messageType",	"key": "messageKey",		"subkey": "optional",   ........ any json data}

Built file is linux64

Usage of msgHub:
  -batchOutput=true: Batch output
  -batchSize=1024: Output batch size
  -batchTimeout="1s": Batch timeout
  -batchingBuffer=1024: Batch buffer size
  -tcpDelimiter=10: TCP message delimiter
  -tcpPort=0: TCP port

