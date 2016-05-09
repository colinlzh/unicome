#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
# import numpy as np
def more(l):
    if l>=3:
        return "1"
    return "0"
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/qiang/features_5*").map(lambda x:(x.split(",")[0],x))
    good=sc.textFile("./kesci/user/effectiveuser.csv").map(lambda x:(x,1))
    line=line.join(good).map(lambda x:x[1][0])
    line.coalesce(1).saveAsTextFile("./kesci/rawdatafilter/features_5")
    line = sc.textFile("./kesci/qiang/features_1*").map(lambda x:(x.split(",")[0],x))
    line=line.join(good).map(lambda x:x[1][0])
    line.coalesce(1).saveAsTextFile("./kesci/rawdatafilter/features_12")

    records=line.map(lambda x:x.split(","))
    t=records.map(lambda x:(x[1],x[7]))\
                    .groupByKey()\
                    .mapValues(list)
    t=t.mapValues(lambda x:len(set(x))).mapValues(more).join(good).map(lambda x:x[0]+","+x[1][0])
    # for i in t.take(5):
    #     print(i)
    t.coalesce(1).saveAsTextFile("./kesci/re1111")
    print("******************OK***********************"+str(t.count())+","+str(t.count()))