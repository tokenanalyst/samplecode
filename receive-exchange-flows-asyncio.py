#!/usr/bin/env python

# Copyright 2019 TokenAnalyst  
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# WS client Python 3.x example to connect to TokenAnalyst websocket API to subscribe to exchange flows

import json
import asyncio
import logging
import websockets
import os 
import sys

logging.basicConfig(level=logging.INFO)

# make sure to export your API key in the environment variables, using $ export API_KEY='...'
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

