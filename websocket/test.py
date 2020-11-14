import asyncio, os
# import uvloop
# uvloop.install()

SLEEP_DURATION = 0  # .5 ms sleep


async def main():
    while True:
        await asyncio.sleep(SLEEP_DURATION)
        os.stat('/tmp')


async def count():
    print("One")
    await asyncio.sleep(0)
    print("Two")


async def main2(s):
    await asyncio.gather(count(), count(), count())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    # asyncio.get_event_loop().create_task(main2(s))
    asyncio.get_event_loop().create_task(main())
    asyncio.get_event_loop().run_forever()
