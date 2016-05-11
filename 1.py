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
    line = sc.textFile("./kesci/userfinalfinal/")
    records=line.map(lambda x:x.split(","))
    records.cache()
    t=records.filter(lambda x:len(x)!=11)
    # records=records.map(fix).map(red)
    # records.saveAsTextFile("./kesci/userfinalfinal")
    # for i in range(12):
    #     if i>=9:
    #         i=str(i+1)
    #     else:
    #         i="0"+str(i+1)
    #     temp=records.filter(lambda x:x.split(",")[0]==("2015"+i))
    #     temp.saveAsTextFile("./kesci/userre/2015"+i)
    # .map(lambda x:(x[0],1)).distinct()
    # t=records.map(lambda x:(x[0],x)).join(t).map(lambda x:x[1][0]).sortBy(lambda x:x[0]+x[1])
    # for i in t.take(12):
    #     print(i)
    # for i in tt.take(12):
    #     print(i)    
    # label.coalesce(1).saveAsTextFile("./kesci/shit")
    print("******************OK***********************"+str(t.count()))