#coding:utf-8  
import sys  
import sys
import time
from operator import add
from pyspark import SparkContext
import operator
import os
import networkx as nx
import numpy as np
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/onetwo/nl2")
    user=line.map(lambda x:x.split(",")[0]).distinct()
    t=line.map(lambda x:(x.split(",")[0],x.replace("\"","")))\
                    .groupByKey()\
                    .mapValues(list).filter(lambda x:len(x[1])>1)
    # t.saveAsTextFile("./kesci/shit0000000000")
    print("******************OK***********************"+str(user.count())+","+str(t.count()))
