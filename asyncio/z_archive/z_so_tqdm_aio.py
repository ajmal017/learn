import asyncio
import random

import tqdm


async def factorial(name, number):
    f = 1
    for i in range(2, number + 1):
        await asyncio.sleep(random.random())
        f *= i
    return f"Task {name}: factorial {number} = {f}"

async def tq(flen):
    for _ in tqdm.tqdm(range(flen)):
        await asyncio.sleep(0.1)


async def main():

    flist = [factorial("A", 2),
             factorial("B", 3),
             factorial("C", 4)]

    pbar = tqdm.tqdm(total=len(flist), position=0, ncols=90)
    for f in asyncio.as_completed(flist):
        value = await f
        pbar.set_description(desc=value, refresh=True)
        tqdm.tqdm.write(value)
        pbar.update()

if __name__ == '__main__':
    asyncio.run(main())
