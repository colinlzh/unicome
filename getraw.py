#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
# import numpy as np
# from pyspark.mllib.regression import LabeledPoint
def timeline(l):
    s=sorted(l,key=lambda x:x[0],reverse=False)
    re=[]
    for i in s:
        re.append(i[1])
    return re
def g2(l):
    s=set()
    for i in l[1]:
        s.add(i[1])
    if len(s)>1:
        return True
    else:
        return False
def lis2str(l):
    re=""
    for i in l[0]:
        re+=str(i)+","
    re+=str(l[1])
    return re
def change1(l,shape):
    cat=np.zeros(shape)
    l=sorted(l,key=lambda x:x[0],reverse=False)
    temp=""
    count=0
    for i in range(shape):
        if i==0:
            temp=l[i][7]
        else:
            if l[i][7]!=temp and l[i][7]!="**":
                count+=1
                cat[i]=1
                temp=l[i][7]
    temp=cat.tolist()
    temp.append(count)
    return temp
def extract_label(l):
    s=set()
    t=[]
    l=sorted(l,key=lambda x:x[0],reverse=False)
    for i in range(5,8):
        s.add(l[i][7])
    if len(s)>1:
        return ("1")
    else:
        return ("0")
def type(l):
     l=sorted(l,key=lambda x:x[0],reverse=False)
     s=set([i[7] for i in l[:6]])
     return len(s)
def toline(l):
    line=""
    line+=l[0]
    for i in l[1]:
        line+=","+i
    return line
def listadd(l):
    l[1].append(l[0])
    return l[1]
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/user/g*")
    records=line.map(lambda x:x.split(","))
    records.cache()
    # train=records.filter(lambda x:x[0]<"201507")
    good=sc.textFile("./kesci/user/effectiveuser.csv").map(lambda x:(x,1))
    t=records.map(lambda x:(x[1],[x[i] for i in [0,3,4,5,6,7]]))\
                    .groupByKey()\
                    .mapValues(list)\
                    .mapValues(lambda x:sorted(x,key=lambda a:a[0]))\
                    .flatMapValues(lambda x:x)
    # label=records.filter(lambda x:x[0]>"201505").map(lambda x:(x[1],x[7]))\
    #                 .groupByKey()\
    #                 .mapValues(list)\
    #                 .mapValues(lambda x:"1" if len(set(x))>1 else "0")
    t=t.join(good).mapValues(lambda x:x[0]).map(toline)
    # for i in t.take(50):
    #     print(i)
    t.coalesce(1).saveAsTextFile("./kesci/newraw")
    print("******************OK***********************"+str(label.count())+","+str(t.count()))
