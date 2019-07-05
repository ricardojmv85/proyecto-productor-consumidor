# IMPORTS
import pandas as pd
import numpy as np 
import threading
import time
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-p", "--producers", required=True)
ap.add_argument("-c", "--consumers", required=True)
ap.add_argument("-b", "--buffer", required=True)
ap.add_argument("-m1", "--matrix1", required=True)
ap.add_argument("-m2", "--matrix2", required=True)
args = vars(ap.parse_args())

# CATCHING ARGUMENTS
producers=str(args["producers"])
consumers=str(args["consumers"])
buffer=str(args["buffer"])
m1=str(args["matrix1"])
m2=str(args["matrix2"])


if __name__ == "__main__":
    # dataframes initialization
    df1=pd.read_csv(m1+'.csv',header=None)
    df2=pd.read_csv(m2+'.csv',header=None)
    print("que onda we")