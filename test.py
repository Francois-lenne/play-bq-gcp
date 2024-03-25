from time import sleep
import asyncio


def background(f):
   def wrapper(*args,**kwargs):
      return asyncio.get_event_loop().run_in_executor(None,f,*args,**kwargs)
   return wrapper

@background
def loop_function(i):
    for j in range(10):
        loop_function2(j)

def loop_function2(i):
    for k in range(10):
        sleep(2)
        loop_function3(k)


def loop_function3(i):
    print(i)
    sleep(2)

loop = asyncio.get_event_loop()
looper = asyncio.gather(*[loop_function(i) for i in range(100) ])
loop.run_until_complete(looper)