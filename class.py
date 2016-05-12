#coding:utf-8  
import sys  
import time
from operator import add
from pyspark import SparkContext
import os
import numpy as np
def processcompare(f):
     dic={};
     for line in f:
         s=line.split(',')#分割字符串
         dic[s[0]]=dic[s[0]]+'|'+s[1] if dic.has_key(s[0]) else s[1]  
     return dic;
def n1(l):
    # 060cf50dcd136163eac4879e10f94e4d,201501,男,50-59,0-49,Apple,iPhone 6 (A1586)
    re=[]
    flag=False
    multi={}
    l=sorted(l,key=lambda x:x[0],reverse=False)
    for i in range(8,12):
        name=l[i][7]
        if "|" in name:
            name=name.split("|")# 需要被替代的名字
            for i in name:
                multi[i]=0
    for i in range(8):
        re.append(l[i])
        name=l[i][7]
        if multi.has_key(name):
            multi[name]=multi[name]+1
    for i in range(8,12):
        name=l[i][7]
        if "|" in name:
            sor=sorted(multi.items(),key=lambda x:x[1],reverse=True)
            l[i][7]=sor[0][0]
        re.append(l[i])
    return re;
def multi(l):
    re=""
    for i in l:
        re+=i+"|"
    return re[:-1]
def jo(l):
    if l[1][1]!=None:
        l[1][0][7]=l[1][1]
    return l[1][0]
def red(l):
    l[7]=l[7].replace("Redmi  3LTE","Redmi 3")\
    .replace("Ascend P7-L09(Ascend P7)","Ascend P7")\
    .replace("N918St(V5 S)","N958St/N918St")\
    .replace("Coolpad 7620L-W00","Coolpad 7620L/Coolpad 7920/Coolpad 7610/Coolpad 7971")\
    .replace("Coolpad 7610","Coolpad 7620L/Coolpad 7920/Coolpad 7610/Coolpad 7971")\
    .replace("Coolpad 9190L","Coolpad 5892")\
    .replace("2700 Classic","2700c/2700c-2")
    line=""
    for i in l:
        line+=i+","
    return line[:-1]
def fix(l):
    if len(l)==11:
        return l
    else :
        if l[7]!=l[-4]: 
            for i in l[7:-3]:
                l[7]=l[7]+"$"+i
        l[8]=l[-3]
        l[9]=l[-2]
        l[10]=l[-1]
        return l[:11]
def red(l):
    line=""
    for i in l:
        line+=i+","
    return line[:-1]    
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/user/g*")
    compare=sc.textFile("./kesci/compare1").filter(lambda x:"???"not in x)\
                .map(lambda x:(x.split(",")[0],x.split(",")[1]))\
                .groupByKey()\
                .mapValues(list)\
                .mapValues(multi)
    records=line.map(lambda x:x.split(","))
    records.cache()
    eight=records.filter(lambda x:x[0]<="201508")
    four=records.filter(lambda x:x[0]>"201508")\
                .map(lambda x:(x[7],x))\
                .leftOuterJoin(compare)\
                .map(jo)
    mul=four.filter(lambda x:"|" in x[7]).map(lambda x:(x[1],1)).distinct()
    four=four.union(eight)
    mul=four.map(lambda x:(x[1],x)).leftOuterJoin(mul)
    normal=mul.filter(lambda x:x[1][1]==None).map(lambda x:x[1][0])
    mul=mul.filter(lambda x:x[1][1]!=None).mapValues(lambda x:x[0])\
                .groupByKey()\
                .mapValues(list)\
                .mapValues(n1)\
                .flatMapValues(lambda x:x)\
                .map(lambda x:x[1])
    mul=mul.union(normal)
    t=mul.map(red).sortBy(lambda x:x.split(",")[1]+x.split(",")[0])
    good=sc.textFile("./kesci/user/effectiveuser.csv").map(lambda x:(x,1))
    t=t.map(lambda x:(x.split(",")[1],x)).join(good).map(lambda x:x[1][0])
    records=t.map(lambda x:x.split(","))
    records.cache()
    t=records.map(fix).map(red)
    for i in range(12):
        if i>=9:
            i=str(i+1)
        else:
            i="0"+str(i+1)
        temp=t.filter(lambda x:x.split(",")[0]==("2015"+i))
        temp.saveAsTextFile("./kesci/userre/2015"+i)
    t.saveAsTextFile("./kesci/userfinal")
    # t=records.map(lambda x:(x[7],x))\
    #                 .groupByKey()\
    #                 .mapValues(list)\
    #                 .join(compare)\
    #                 .map(jo)
    # t=t.mapValues(lambda x:beginreplace(x,mydic)).flatMapValues(lambda x:x).map(lambda x:x[1]).sortBy(lambda x:x[0]+x[1])
    # for i in t.take(1200):
    #     print(i)
    # line=line.sortBy(lambda x:x.split(",")[0])
    # t.saveAsTextFile("./kesci/finalin11")
    # line.saveAsTextFile("./kesci/hei")
    print("******************OK***********************"+str(t)+","+str(t))