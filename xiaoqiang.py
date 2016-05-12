#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
import numpy as np
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
def change1(l,shape,flag):
    cat=np.zeros(shape)
    l=sorted(l,key=lambda x:x[0],reverse=False)
    temp=""
    count=0
    for i in range(shape):
        if i==0:
            temp=l[i][1]
        else:
            if l[i][1]!=temp and l[i][1]!="**":
                count+=1
                cat[i]=1
                temp=l[i][1]
    temp=cat.tolist()
    temp.append(count)
    s=set([i[1] for i in l[:shape]])
    temp.append(len(s))
    last=l[shape-1][1]
    usetime=0
    for i in range(shape):
        if last==l[i][1]:
            usetime=shape-i
    temp.append(usetime)
    if flag:
        s=set([i[1] for i in l[shape-1:shape+3]])
        a=1 if len(s)>1 else 0
        temp.append(a)
    return temp
def toline(l):
    line=""
    line+=l[0]
    for i in l[1]:
        line+=","+str(i)
    return line
def listadd(l):
    l[1].append(l[0])
    return l[1]
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/userfinal/")
    records=line.map(lambda x:x.split(","))
    records.cache()
    # records=records.filter(lambda x:x[0]>"201503")
    t=records.map(lambda x:(x[1],[x[i] for i in [0,7]]))\
                    .groupByKey()\
                    .mapValues(list)\
                    .mapValues(lambda x:change1(x,9,True))\
                    .map(toline)
    # for i in t.take(50):
    #     print(i)
    t.coalesce(1).saveAsTextFile("./kesci/19")
    # print("******************OK***********************"+str(label.count())+","+str(t.count()))
