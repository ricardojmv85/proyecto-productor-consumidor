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
filled = Semaphore(0)
empty = Semaphore(buffer) 

# PRODUCER FUNCTION
def producer_function(name):
    print(name," init")
    global tasks, result, buffer
    while True:
        # LOCKING THE POOL OF TASKS TO OBTAIN A ROW/COLUMN TASK
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
                # INSERTING MULTI ITEM TO THE BUFFER
                item = [task[0][i],task[1][i],task[2]]
                # SUBS 1 TO EMPTY SPACES IN BUFFER
                empty.acquire()
                # SUBS 1 TO ACCESS THE BUFFER
                buffer_access.acquire()
                # INSERTING ITEM TO BUFFER
                print(name, " inserting into buffer ",item)
                buffer.append(item)
                # ADDING 1 TO THE BUFFER ACCES
                buffer_access.release()
                # ADDING 1 TO THE FILLED SPACES IN BUFFER
                filled.release()



# CONSUMER FUNCTION
def consumer_function(name):
    print(name," init")
    global tasks, result, finish, buffer
    while True:
        # SUBS 1 TO FILLED SPACES IN BUFFER
        filled.acquire()
        # SUBS 1 TO ACCESS THE BUFFER
        buffer_access.acquire()
        # EXTRACTING THE FIRST ITEM ON THE BUFFER
        item=buffer.pop(0)
        # ADDING 1 TO THE BUFFER ACCESS
        buffer_access.release()
        # ADDING 1 TO THE EMPY SPACES IN BUFFER
        empty.release()
        # CONSUMING THE ITEM MULTIPLICATION
        result=item[0]*item[1]
        # SENDING TO MYSQL
        print(name, " sending ",result,item[2])

        


if __name__ == "__main__":
    # dataframes initialization
    df1=pd.read_csv(m1+'.csv',header=None)
    df2=pd.read_csv(m2+'.csv',header=None)
    tasks = []
    buffer=[]
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
    for i in range(consumers):
        x = threading.Thread(target=consumer_function, args=(i,))
        x.start()
    #     # x.join()
    while(finish):
        True
    
    