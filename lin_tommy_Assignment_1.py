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

    #Task 1
    #Your code goes here
    
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

    # Gets rid of duplicate taxi + driver combinations using map and distinct
    # Turn each unique taxi driver into tuple of single count using map
    # Add up total number of drivers per taxi using reduceByKey
    # Get top ten using takeOrdered
    top_ten_taxis = filtered_data\
        .map(lambda x: (x[0], x[1]))\
        .distinct()\
        .map(lambda x: (x[0], 1))\
        .reduceByKey(lambda x, y: x + y)\
        .takeOrdered(10, key=lambda x: -x[1])
    
    # Output results
    results_1 = sc.parallelize(top_ten_taxis)
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])


    #Task 2
    #Your code goes here

    # 1. Get necessary columns using map
    # 2. Sum up total amount and total time spent for each driver using reduceByKey
    # 3. Calculate amount/time for each driver using map
    # 4. Get top ten using takeOrdered
    driver_money_per_minute = filtered_data\
        .map(lambda x: (x[1], [float(x[16]), float(x[4]) / 60]))\
        .reduceByKey(lambda x, y: ([x[0] + y[0], x[1] + y[1]]))\
        .map(lambda x: (x[0], x[1][0]/x[1][1]))\
        .takeOrdered(10, lambda x: -x[1])

    #savings output to argument
    results_2 = sc.parallelize(driver_money_per_minute)
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()