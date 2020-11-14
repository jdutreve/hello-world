import asyncio
import websockets


async def hello():
    uri = "ws://127.0.0.1:5678"
    async with websockets.connect(uri) as websocket:
        while True:
            greeting = await websocket.recv()
            print(f"< {greeting}")

asyncio.get_event_loop().run_until_complete(hello())
