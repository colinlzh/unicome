#coding:utf-8  
import sys
from numpy import *
import time
import pylab as plt
import numpy as np


if __name__ == "__main__":
    n=[0]
    l=[0]
    i =0
    c=0
    left=[]
    right=[]
    with open("up.csv",'rb') as ifile:
        for line in ifile:
            line=line.decode('utf-8').strip("\r\n")
            t=line.split(",")
            if len(t[1])>7:
                line=t[0]+","+t[1][:7]+"\r"
            else:
                line=t[0]+","+t[1]+"\r"
            left.append(line)
            # if c==100:
            #     break
            # c+=1
    left=[]
    for i in range(100000):
        left.append(i)
    w=open("test","w")
    w.writelines(left)
    w.close()
    #         t=line.split(":")
    #         s=line.replace(":"+t[-1],"")
    #         left.append(s)
    # # print(c)
    # with open("right",'rb') as ifile:
    #     for line in ifile:
    #         line=line.decode('utf-8')
    #         t=line.split(":")
    #         s=line.replace(":"+t[-1],"")
    #         right.append(s)
    # for i in left:
    #     testi=i.lower()
    #     print i
    #     if c==10:
    #         break
    #     c+=1
    # c=0
    # i=0
    # with open("l",'rb') as ifile:
    #     for line in ifile:
    #         line=line.decode('utf-8')
    #         t=line.split("|")
    #         i+=1
    #         l.append(int(t[1])**(0.3))
    # j=0
    # for i in range(1,1000,5):
    #     j+=1
    #     print(str(j)+","+str(l[i]))
    # plt.plot(range(len(n)),n,'r',label='before cluster',linewidth=2)
    # plt.plot(range(len(l)),l,'b',label='after cluster',linewidth=2)
    #
    # all=[]
    # for i in range(len(n)):
    #     all.append(787898)
    # plt.plot(range(len(all)),all,'y',label='total number',linewidth=1.5,linestyle='dashed')
    # line=[]
    # for i in range(1188):
    #     line.append(0)
    # line.append(787898)
    # plt.plot(range(len(line)),line,'c',linestyle='dashed')
    # # plt.xscale('log')
    # plt.legend(loc=10,fontsize='x-large')
    # plt.xlabel("Motif Index",fontsize='x-large')
    # plt.ylabel("Motif Number",fontsize='x-large')
    # plt.show()
