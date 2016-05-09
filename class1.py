#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
# import numpy as np
def standalize(l):
    l[2]=l[2].replace(u"男",1).replace(u"女",0).replace(u"不详",2)
    l[4]=l[4].replace(u"及以上","")
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/newraw/")
    compare=sc.textFile("./kesci/compare").filter(lambda x:"??" in x).map(lambda x:x.split(",")[0]).collect()
    records=line.map(lambda x:x.split(","))
    records.cache()
    for i in compare:        
        s=records.filter(lambda x:x[6]==i).map(lambda x:(x[0],1)).distinct()
        s=records.map(lambda x:(x[0],x)).join(s).map(lambda x:x[1][0])
        s.saveAsTextFile("kesci/multi/"+i)
    # t=records.filter(lambda x:x[1]>"201508")
    # label=t.map(lambda x:(x[4],1)).reduceByKey(add).sortBy(lambda x:x[1],False).map(lambda x:x[0]+":"+str(x[1]))
    # cat=t.map(lambda x:(x[5],1)).reduceByKey(add).sortBy(lambda x:x[1],False).map(lambda x:x[0]+":"+str(x[1]))
    # label.coalesce(1).saveAsTextFile("./kesci/label")
    # cat.coalesce(1).saveAsTextFile("./kesci/cat")
    # print("******************OK***********************"+str(t.count())+","+str(t.count()))
