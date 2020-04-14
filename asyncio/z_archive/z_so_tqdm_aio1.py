# Working program Ref SO question: https://stackoverflow.com/questions/61116000/ 
# Uses example of asyncio.run and asyncio.as_completed with tqdm
# Illustrates the difference between the two.

import asyncio
import random

import tqdm

NUMBERS = 9


async def say_after(delay, i):
    await asyncio.sleep(delay)
    print(f"\nprinted item no {i}\n")
    return f"returned item no {i}"
    
async def coro():
    tasks = [say_after(random.randint(1, 2), i) for i in range(NUMBERS)]
    return await asyncio.gather(*tasks)

result1 = asyncio.run(coro())
print(f"{result1}\n")

async def coro_tqdm():
    tasks = [asyncio.ensure_future(say_after(random.randint(1, 2), i)) 
             for i in range(NUMBERS)]
    
    answer = []
    pbar = tqdm.tqdm(total=len(tasks), position=0, ncols=120)
    for future in asyncio.as_completed(tasks):
        value = await future
        answer.append(value)
        
        pbar.set_description(desc=f'Returned {value[-1]} \n', refresh=True)
        pbar.update()
        
    # answer = [await j for j in tqdm.tqdm(asyncio.as_completed(tasks))] # alternative
    
    return answer
    
result2 = asyncio.run(coro_tqdm())
print(result2)
