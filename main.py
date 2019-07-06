# IMPORTS
import pandas as pd
import numpy as np 
import threading
import time
import argparse
from threading import Lock, Semaphore

ap = argparse.ArgumentParser()
ap.add_argument("-p", "--producers", required=True)
ap.add_argument("-c", "--consumers", required=True)
ap.add_argument("-b", "--buffer", required=True)
ap.add_argument("-m1", "--matrix1", required=True)
ap.add_argument("-m2", "--matrix2", required=True)
args = vars(ap.parse_args())

# CATCHING ARGUMENTS
producers=int(args["producers"])
consumers=int(args["consumers"])
buffer=int(args["buffer"])
m1=str(args["matrix1"])
m2=str(args["matrix2"])

lock=Lock()
buffer_access = Semaphore(1)
spaces_to_fill = Semaphore(0)
empty_spaces = Semaphore(buffer) 

# PRODUCER FUNCTION
def producer_function(name):
    print(name," init")
    global tasks, result
    while True:
        lock.acquire()
        try:
            task=tasks[0]
            tasks.pop(0)
        except:
            task=""
        lock.release()
        if task=="":
            break
        else:
            for i in range(len(task[0])):
                item = [task[0][i],task[1][i]]
                empty_spaces.acquire()
                buffer_access.acquire()
                print(name, " inseting into buffer ",item)
                buffer_access.release()
                spaces_to_fill.release()



# CONSUMER FUNCTION
def consumer_function(name):
    print(name," init")
    global tasks, result, finish
    while True:
        lock.acquire()
        try:
            task=tasks[0]
            tasks.pop(0)
        except:
            task=""
        lock.release()
        if task=="":
            finish=False
            break
        else:
            print(name, " consuming")


if __name__ == "__main__":
    # dataframes initialization
    df1=pd.read_csv(m1+'.csv',header=None)
    df2=pd.read_csv(m2+'.csv',header=None)
    tasks = []
    finish=True
    # TASK CREATION
    for i in range(len(df1)):
        for j in range(len(df2)):
            tasks.append([list(df1.iloc[i]),list(df2[j]),[i,j]])
 
    # CREATING PRODUCERS
    for i in range(producers):
        x = threading.Thread(target=producer_function, args=(i,))
        x.start()
    # CREATING CONSUMERS
    # for i in range(consumers):
    #     x = threading.Thread(target=consumer_function, args=(i,))
    #     x.start()
    #     # x.join()
    while(finish):
        True
    
    