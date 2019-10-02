#!/usr/bin/env python

import json
import asyncio
import logging
import websockets
import os 
import sys
import queue
import asyncio
import time

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ['API_KEY']



# Blockchain.com Protocol

def bcom_block_converter(payload):
    data  = json.loads(payload) 
    if(data['op'] == 'block'):
        ts = data['x']['time']
        blockNumber = data['x']['height']
        return(block(ts, blockNumber))
    else:
        None

blockchain = {
    "id": "Blockchain.com",
    "url": "wss://ws.blockchain.info/inv",
    "subscribe_json": json.dumps({"op":"blocks_sub"}), 
    "block_converter": bcom_block_converter
}

# TokenAnalyst Protocol

def ta_block_converter(payload):
    data = json.loads(payload)
    if(data['event'] == 'data'):
        ts = data['data']['timestamp']
        blockNumber = data['data']['blockNumber']
        return(block(ts, blockNumber))
    else:
        None
ta = {
    "id": "TokenAnalyst",
    "url": "ws://ws.tokenanalyst.io:8000",
    "subscribe_json": json.dumps({"event":"subscribe","channel":"exchange_flows","id":"0","key":API_KEY}),
    "block_converter": ta_block_converter 
}

def block(timestamp, blocknumber):
    return({'seen':int(time.time()*1000), 'blockNumber':blocknumber, 'timestamp': timestamp*1000})

async def subscribe(protocol):
  uri = protocol['url']
  highest_block = 0
  socket = await websockets.connect(uri)
  await socket.send(protocol['subscribe_json'])

  async for msg in socket:
    block = protocol['block_converter'](msg)
    if(block != None and block['blockNumber'] > highest_block): #dedup
        highest_block = block['blockNumber']
        print({
            "id":protocol['id'], 
            "blockNumber": block['blockNumber'],
            "timestamp": block['timestamp'],
            "seen":block['seen']})

async def run_both():
    F1 = subscribe(ta)
    F2 = subscribe(blockchain)
    await asyncio.gather(F1, F2)

asyncio.run(run_both())