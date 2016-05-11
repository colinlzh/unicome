#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
import numpy as np
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/newraw/").map(lambda x:x.split(",")[0]+","+x.split(",")[1]+","+x.split(",")[5]+","+x.split(",")[6])
    compare=sc.textFile("./kesci/compare1").filter(lambda x:"??" in x).map(lambda x:x.split(",")[0]).collect()
    records=line.map(lambda x:x.split(","))
    records.cache()
    for i in range(100):        
        s=records.filter(lambda x:x[3]==compare[i]).map(lambda x:(x[0],1)).distinct()
        s=records.map(lambda x:(x[0],x)).join(s).map(lambda x:x[1][0])
        s.saveAsTextFile("kesci/multi/"+str(1000+i)+compare[i])
    # t.coalesce(1).saveAsTextFile("./kesci/re11")
    print("******************OK***********************"+str(t.count())+","+str(t.count()))