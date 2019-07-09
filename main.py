# IMPORTS
import pandas as pd
import numpy as np 
import threading
import time
import argparse
import pymysql
from datetime import datetime
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

DB_NAME='mazinger'
DB_USER='root'
DB_PASS='mazinger123'
DB_HOST='mazinger.cd3weixjwsqp.us-east-2.rds.amazonaws.com'
DB_PORT=3306
conn = pymysql.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
mycursor = conn.cursor()

lock=Lock()
lock2=Lock()
buffer_access = Semaphore(1)
filled = Semaphore(0)
empty = Semaphore(buffer) 

# PRODUCER FUNCTION
def producer_function(name):
    print(name," init")
    global tasks, buffer
    while True:
        # LOCKING THE POOL OF TASKS TO OBTAIN A ROW/COLUMN TASK
        lock.acquire()
        try:
            task=tasks.pop(0)
            t1 = datetime.now()
        except:
            task=""
        lock.release()
        if task=="":
            break
        else:
            for i in range(len(task[0])):
                # INSERTING MULTI ITEM TO THE BUFFER
                item = [task[0][i],task[1][i],task[2],t1]
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
    global tasks, finish, buffer
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
        res=item[0]*item[1]
        # SENDING TO MYSQL
        print(name, " sending ",res,item[2])
        lock2.acquire()
        mycursor.execute('update mazinger.results set result=result+"'+str(res)+'" , time=time+"'+str(int((item[3]-datetime.now()).microseconds/1000))+'" where results.row="'+str(item[2][0])+'" and results.column ="'+str(item[2][1])+'"')
        conn.commit()
        lock2.release()

        


if __name__ == "__main__":
    # dataframes initialization
    df1=pd.read_csv(m1+'.csv',header=None)
    df2=pd.read_csv(m2+'.csv',header=None)
    tasks = []
    buffer=[]
    finish=True
    # TASK CREATION AND MYSQL TABLE INIT
    sql = 'Truncate table mazinger.results'
    mycursor.execute(sql)
    conn.commit()
    for i in range(len(df1)):
        for j in range(len(df2)):
            tasks.append([list(df1.iloc[i]),list(df2[j]),[i,j]])
            sql = 'INSERT INTO mazinger.results VALUES ("'+str(i)+'", "'+str(j)+'", "'+str(0)+'", "'+str(0)+'")'
            mycursor.execute(sql)
            conn.commit()

 
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
    
    