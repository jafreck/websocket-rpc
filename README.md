# WebsocketRPC
This project is a proof of concept for implementing RPC over websockets.

# Getting Started

You can run the interactive client server websocket test by executing the following two commands in order:
- `python src/rpc/main.py server`
- `python src/rpc/main.py client`

The client program will prompt for input messages, which it will send to the server. The server will respond in the pattern:
{input_message}/answer.
