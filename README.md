msgHub
======

Route JSON based messages over TCP

run hub up using:

msgHub -tcpPort=[port] -tcpDelimiter=[ascii code]

e.g. start on 12345 using line feed as delimiter

msgHub -tcpPort=12345 -tcpDelimiter=10

JSON message format:

{"event": "sub",	"type": "messageType",	"key": "messageKey",		"id": "optional id. used for same sub down same tcp client"}
{"event": "unsub",	"type": "messageType",	"key": "messageKey",		"id": "optional id. used for same sub down same tcp client"}
{"event": "pub",	"type": "messageType",	"key": "messageKey",		........ any json data}


