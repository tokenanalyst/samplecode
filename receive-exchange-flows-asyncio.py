#!/usr/bin/env python

# WS client example to connect to TokenAnalyst websocket API to subscribe to exchange flows

import json
import asyncio
import logging
import websockets
import os 
import sys

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ['API_KEY']

async def interpret(response, id, on_data, on_heartbeat, on_subscribed):
  if(response['id'] == id and response['event'] == "data"):
    await on_data(response['data'])
  if(response['id'] == None and response['event'] == "heartbeat"):
    await on_heartbeat(response['data'])
  if(response['id'] == id and response['event'] == "subscribed" and response['data']['success'] == True):
    await on_subscribed(response['data'])

async def subscribe(key, on_data, on_heartbeat, on_subscribed):
  uri = "ws://ws.tokenanalyst.io:8000"
  id = "0"
  channel = "exchange_flows"
  payload = {"event":"subscribe","channel":channel,"id":id,"key":key}
  async with websockets.connect(uri) as websocket:
    await websocket.send(json.dumps(payload))
    async for msg in websocket: 
      await interpret(json.loads(msg), id, on_data, on_heartbeat, on_subscribed)

async def on_data(data): 
  print("Received data: " + str(data))

async def on_heartbeat(heartbeat):
   print("Received heartbeat: " + str(heartbeat)) 

async def on_subscribed(details):
   print("Successfully subscribed: " + str(details))

asyncio.run(subscribe(API_KEY, on_data, on_heartbeat, on_subscribed))

