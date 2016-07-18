#!/usr/bin/env python
# -*- coding: utf-8 -*-

import string


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json


from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from helper import *
from nltk import FreqDist



import rdflib
from rdflib import Namespace
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import RDF, FOAF
from rdflib import Graph
#from nltk.book import *


rdfGraph = rdflib.Graph()
namespc = Namespace("http://dbpedia.org/resource/")


def insertTriple(x):

	#st = namespc+"."+y
	uriNode = URIRef(x[1])
	#print uriNode
	
	for ele in x[0]:
		global rdfGraph
		k = BNode()
		rdfGraph.add((k,FOAF.Person,Literal(ele[0])))
		rdfGraph.add((k,FOAF.age,Literal(ele[1])))
		#print ele[0]
		rdfGraph.add((k,FOAF.primaryTopic,uriNode))

	#print rdfGraph.serialize(format='n3')


conf = SparkConf().setMaster("local").setAppName("Dbpedia1")

sc = SparkContext(conf = conf)

raw1 = sc.textFile("/home/ramkarnani/wikiextractor-master/rewikifile/wiki00")

raw2 = raw1.map(lambda x: json.loads(x)) 

raw3 = raw2.map(lambda x:(x['url'], x['text']))





#reading stopWords
#stopWords = readStopWords("stopword.txt")
#stopWord = sc.broadcast(stopWords)
#print stopWords[0]
#forming punctuation symbols
#pSet = set(string.punctuation)
#punctSet = sc.broadcast(pSet)
#print pSet
#proc1 = raw3.map(lambda (x,y):(x,processing(y,stopWord,punctSet)))
str = "http://dbpedia.org/resource/"

raw4 = raw3.map(lambda (x,y):(x,y, (str+ (((y.split("\n")[0]).strip())[:-1]).replace(" ","_")    )     ))
#raw4 = raw3.map(lambda (x,y):(x,y, ((((y.split("\n")[0]).strip())[:-1]).replace(" ","_")    )     ))

proc1 = raw4.map(lambda (x,y,z):(x,processing(y),z))
#print proc1.first()

#proc2 = proc1.map(lambda (x,y,z):(x, [w[0] for w in ( ((FreqDist(y)).most_common(100)) )],z))
#proc2 = proc1.map(lambda (x,y,z):(x, FreqDist(y).most_common(100) ,z))
proc2 = proc1.map(lambda (x,y,z):(FreqDist(y).most_common(100) ,z))
#proc3 = proc2.map(lambda (x,y,z): (("wikiurl",x),("text",y),("dburl",z)))




proc2.foreach(insertTriple)
print rdfGraph.serialize(format='n3')

#proc4 = (proc5.saveAsTextFile("outputRDF"))

# s = URIRef(namespc.anarchism)
# k = BNode()

# g.add((k,FOAF.Person,Literal("Berlin")))
# g.add((k,FOAF.age,Literal("21")))
# g.add((k,FOAF.primaryTopic,s))

#print g.serialize(format='n3')

#proc4 = (proc2.saveAsTextFile("outputRDF"))





# g.add((k,FOAF.Person,Literal("Berlin")))
# g.add((k,FOAF.age,Literal("21")))
# g.add((k,FOAF.primaryTopic,s))
# print g.serialize(format='n3')








#print dict((proc2.first())[1])['balkan']

#print proc2.first()

#processing(sc,)


#print raw3.first()

#sqlctx = SQLContext(sc)
#wikiFile = sqlctx.read.json("/home/ramkarnani/wikiextractor-master/rewikifile/wiki00")
#wikiFile.registerTempTable("wikiFile")
#rawUrl = sqlctx.sql("SELECT url from wikiFile")
#rawUrl1 = rawUrl.rdd().map(lambda x:x)
#raw = sqlctx.sql("SELECT text from wikiFile").rdd().map(lambda x:x)
#raw


#wikiFile.show()
#result = sqlctx.sql("SELECT id from wikiFile")

#result.show()


