from threading import Thread 
import multiprocessing
import urllib2, urllib ,string
import json
import time
from collections import OrderedDict

def hello():
	j=0
	while j!=3:
		counter=0	
		temp=list()

		i=0
		while i!=4:
			if i%2==0:
				temp.append("a...b")
			else :
				temp.append("b")
			#print "\n"
			print temp[i]
			i=i+1
		j=j+1	

hello()    