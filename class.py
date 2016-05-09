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
         s=line.split(',');#分割字符串
         dic[s[0]]=dic[s[0]]+','+s[1] if dic.has_key(s[0]) else s[1]  
     return dic;
def beginreplace(hello,dic):
    hello=sorted(hello,lambda x:x[1])
    count={};
    hello_1=[];
    dics=[];
    for i in dic.values():
        l=i.split(',');
        for j in l:
            dics.append(j)
    for i in range(8):
        listline=hello[i];
        linesplit=listline.split(',');
        s=linesplit[6][3:-3];
        #print s
        #print s in dics
        if s in dics:#判断字典中是否存在这个值
            if count.has_key(s):
               count[s]=count[s]+1;
            else:
               count[s]=1;
        else:
            pass;
        hello_1.append(listline);
    #print type(count)
    for i in range(4):
        listline=hello[i+8];
        listline1=listline.strip('\n');
        linesplit=listline1.split(',');
        #print linesplit
        s=linesplit[6][3:-2];
        #print s
        if dic.has_key(s):#存在对照表
            if ',' in dic[s]:#关键字存在多个选项
                if count:#前八个里面有候选值，取出现次数最多的候选值
                    #print type(count)
                    count_num= sorted(count.iteritems(), key=lambda d:d[1], reverse = True)
                    listline=listline.replace(s,count_num[0][0]);
                    #hello_1.append(listline);
                else:#候选值都不在前八个里面,取对照表第一个
                    candidate=dic[s].split(',');
                    listline=listline.replace(s,candidate[0]);
                    #hello_1.append(listline);
            else:#关键字只有一个选项，直接替换
                listline=listline.replace(s,dic[s]);
                #hello_1.append(listline);
        else:#不存在对照表，不改变，保留原值
            pass;
            #hello_1.append(listline);
        hello_1.append(listline);
    #print hello_1;
    #print type(hello_1)
    return hello_1;
if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("./kesci/newraw")
    compare=sc.textFile("./kesci/compare").collect()
    mydic=processcompare(compare)
    # records=line.map(lambda x:x.split(","))
    t=line.map(lambda x:(x.split(",")[0],x))\
                    .groupByKey()\
                    .mapValues(list)
    t=t.mapValues(lambda x:beginreplace(x,mydic))
    for i in t.take(960):
        print(i)
    # t.coalesce(1).saveAsTextFile("./kesci/re11")
    print("******************OK***********************"+str(t.count())+","+str(t.count()))