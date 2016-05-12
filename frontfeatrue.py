#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
import numpy as np
# import numpy as np
# from pyspark.mllib.regression import LabeledPoint
age = {u'未知':0,u'17岁以下':17, u'18-22':20,u'23-25':24,u'26-29':27.5,'30-39':35,'40-49':45.5,'50-59':55.5,u'60以上':60}
ARPU = {'0-49':24.5,'50-99':74.5,'100-149':124.5,'150-199':174.5,'200-249':224.5,'250-299':274.5,u'300及以上':300}
flow =  {'0-499':249.5,'500-999':749.5,'1000-1499':1249.5,'1500-1999':1749.5,'2000-2499':2249.5,'2500-2999':2749.5,'3000-3499':3249.5,'3500-3999':3749.5,'4000-4499':4249.5,'4500-4999':4749.5,u'5000以上':5000}
sex = {u'男':0,u'女':1,u'不详':2}
net = {'3G':0,'2G':1}
def extract_feature(l):
    s=set()
    t=[]
    l=sorted(l,key=lambda x:x[0],reverse=False)
    t.append(l[0][1])
    for j in range(9):
        i=l[j]
        temp=[net[i[2]],sex[i[3]],age[i[4]],ARPU[i[5]],flow[i[8]],float(i[9]),float(i[10])]
        t.extend(temp)
        cat=np.zeros(1001)
        if i[7] in ss.keys():
            cat[ss[i[7]]]=1
        t.extend(cat.tolist())
    line=""
    for i in t:
        line+=str(i)+","
    return line[:-1]
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/userfinal")
    records=line.map(lambda x:x.split(","))
    records.cache()
    top=sc.textFile("./kesci/top4000")\
                    .map(lambda x:x.split(":")[0])\
                    .zipWithIndex().collect()
    ss={}
    for i in top:
        ss[i[0]]=i[1]
    t=records.map(lambda x:(x[1],x))\
                    .groupByKey()\
                    .mapValues(list)\
                    .mapValues(extract_feature)\
                    .map(lambda x:x[1])
    t.saveAsTextFile("./kesci/pcapre9_1000")
    print("******************OK***********************")
