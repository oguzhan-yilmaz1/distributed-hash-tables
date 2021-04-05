# Tests for dht
from dht import *
from random import randint
import hashlib


d = DHT(12)

nodes_file = open("nodes.txt",'r')
lines = nodes_file.readlines()
nodes_file.close()
for line in lines:
    if line[-1]=='\n':
        line=line[:-1]
    n=Node(int(hashlib.sha256(line.encode('utf-8')).hexdigest(),16)%d._size)
    d.join(n)
d.updateAllFingerTables();
nodes_file = open("files.txt",'r')
lines = nodes_file.readlines()
nodes_file.close()
for line in lines:
    if line[-1]=='\n':
        line=line[:-1]
    d.store(d._startNode, int(hashlib.sha256(line.encode('utf-8')).hexdigest(),16), line)

f = open("ring_structure.csv",'w')
numNodes=1
nodeCurr=d._startNode
while numNodes<=d._numNodes:
    f.write('{}'.format(nodeCurr.ID))
    for dat in nodeCurr.data.values():
        f.write('\t{}'.format(dat))
    f.write('\n')
    numNodes=numNodes+1
    nodeCurr=nodeCurr.fingerTable[0]
f.close()

f2=open("queries.txt",'r')
lines = f2.readlines()
f2.close()
f = open("routes.csv",'w')
for line in lines:
    if line[-1]=='\n':
        line=line[:-1]
    delimed = line.split(",")
    startNodeKey=int(hashlib.sha256(delimed[0].encode('utf-8')).hexdigest(),16)
    objectKey = int(hashlib.sha256(delimed[1].encode('utf-8')).hexdigest(),16)
    routes = d.query(d.findNode(d._startNode,startNodeKey),objectKey)
    for route in routes:
        f.write('{}\t'.format(route.ID))
    if objectKey not in routes[-1].data:
        f.write('None')
    f.write('\n')
f.close()
