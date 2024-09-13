from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p
            
#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])
    
    # Splits string into array of values by comma separator
    taxi_data = rdd.map(lambda x: x.split(','))

    # This filters out zero values for total amount, total fare, and distance traveled
    # Also filters out rides that lasted less than a minute
    filtered_data = taxi_data.filter(correctRows)

    # Filters out rides that have a 0.0 in the lat/long coordinates (rides are in NYC)
    # Filters out rides less than 60 seconds, rides less than 0.1 miles, and rides that cost less than $1
    # Filters out rides that have a smaller total cost compared to fare cost which is impossible
    filtered_data = filtered_data\
        .filter(lambda x: float(x[6]) * float(x[7]) * float(x[8]) * float(x[9]) > 0)\
        .filter(lambda x: float(x[5]) >= 0.1 and float(x[4]) >= 60 and float(x[16]) >= 1)\
        .filter(lambda x: float(x[16]) >= float(x[11]))


    # Task 3
    # Got hour of day as well as surcharge and distance
    # Summed surcharge and distance by hour using reduceByKey
    # Used map to calculate surcharge/distance for each hour
    # Got top ten results
    best_time_data = filtered_data\
        .map(lambda x: (x[2].split(" ")[1].split(":")[0], [float(x[12]), float(x[5])]))\
        .reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])\
        .map(lambda x: (x[0], x[1][0]/x[1][1]))\
        .takeOrdered(10, lambda x: -x[1])
    
    print("Top 10 Time Profit Ratio:")
    for data in best_time_data:
        print(data)

    # Task 4a
    # Filtered out non-cash and non-credit payments
    # Key'ed data by hour of day and payment method
    # Summed CRD and CSH payments by hour using reduceByKey
    # Calculated CRD percentage by hour using CRD / (CRD + CSH)
    # Reformatted, sorted, and got the results
    payment_data = filtered_data\
        .filter(lambda x: x[10] == "CSH" or x[10] == "CRD")\
        .map(lambda x: ((x[2].split(" ")[1].split(":")[0], x[10]), 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .reduceByKey(lambda x, y: (x[0], x[1] / (x[1] + y[1])) if x[0] == "CRD" else (y[0], y[1] / (y[1] + x[1])))\
        .map(lambda x: (x[0], x[1][1]))\
        .sortByKey()\
        .collect()
    
    print("\n(Hour, Percentage CRD)")
    for data in payment_data:
        print(data)

    # Task 4c
    # Got tip data
    # Sorted data to help find quartiles
    tip_data = filtered_data\
        .map(lambda x: float(x[14]))\
        .sortBy(lambda x: x)\
        .collect()
    
    tip_mean = sc.parallelize(tip_data, numSlices=20).mean()
    tip_median = sc.parallelize(tip_data, numSlices=20)\
        .zipWithIndex()\
        .filter(lambda x: x[1] == int(len(tip_data)/2))\
        .map(lambda x: x[0])\
        .collect()
    first_q = sc.parallelize(tip_data, numSlices=20)\
        .zipWithIndex()\
        .filter(lambda x: x[1] == int(len(tip_data)/4))\
        .map(lambda x: x[0])\
        .collect()
    third_q = sc.parallelize(tip_data, numSlices=20)\
        .zipWithIndex()\
        .filter(lambda x: x[1] == int(len(tip_data)/4*3))\
        .map(lambda x: x[0])\
        .collect()
    
    print(f"\nMean: {tip_mean}")
    print(f"Median: {tip_median}")
    print(f"1st Q: {first_q}")
    print(f"3rd Q {third_q}")


    sc.stop()