# Tests for dht
from dht import *
from random import randint
import hashlib
import matplotlib.pyplot as plt
import numpy as np

d = DHT(12)

nodes_file = open("nodes_part1b.txt",'r')
lines = nodes_file.readlines()
nodes_file.close()

for line in lines:
    if line[-1]=='\n':
        line=line[:-1]
    n=Node(int(hashlib.sha256(line.encode('utf-8')).hexdigest(),16)%d._size)
    d.join(n)
d.updateAllFingerTables();

nodes_file = open("files_part1b.txt",'r')
lines = nodes_file.readlines()
nodes_file.close()
for line in lines:
    if line[-1]=='\n':
        line=line[:-1]
    d.uniqueStore(d._startNode, int(hashlib.sha256(line.encode('utf-8')).hexdigest(),16), line)
    if d._uniqueStoredKeyValueCount==1024:
        print("Success 1b")
        break


numNodes=1
nodeCurr=d._startNode
numFiles=[]
nodeIDs=[]
while numNodes<=d._numNodes:
    numFiles.append(len(nodeCurr.data))
    nodeIDs.append(nodeCurr.ID)
    numNodes=numNodes+1
    nodeCurr=nodeCurr.fingerTable[0]

meanfiles=np.mean(numFiles)
sigma=np.std(numFiles)
textstr = '\n'.join((
    r'$\mu=%.2f$' % (meanfiles, ),
    r'$\sigma=%.2f$' % (sigma, )))
x = np.arange(len(nodeIDs))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x, numFiles, width, label='Men')
props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Number of filenames')
ax.set_title('Number of filenames stored at each node')
ax.set_xticks(x)
ax.set_xticklabels(nodeIDs)
ax.text(0.05, 0.95, textstr, transform=ax.transAxes, fontsize=14,
        verticalalignment='top', bbox=props)
plt.xticks(rotation='vertical')

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
autolabel(rects1)
plt.show()


f2=open("queries_part1b.txt",'r')
lines = f2.readlines()
f2.close()
numHops=[]
for line in lines:
    if line[-1]=='\n':
        line=line[:-1]
    delimed = line.split(",")
    startNodeKey=int(hashlib.sha256(delimed[0].encode('utf-8')).hexdigest(),16)
    objectKey = int(hashlib.sha256(delimed[1].encode('utf-8')).hexdigest(),16)
    routes = d.query(d.findNode(d._startNode,startNodeKey),objectKey)
    numHops.append(len(routes)-1)
meanhops=np.mean(numHops)
sigma=np.std(numHops)
textstr = '\n'.join((
    r'$\mu=%.2f$' % (meanhops, ),
    r'$\sigma=%.2f$' % (sigma, )))
x = np.arange(50)  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x, numHops, width, label='Men')
props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Number of hops')
ax.set_title('Number of hops for each query (x axis in query number in the list of queries)')
ax.set_xticks(x)
ax.set_xticklabels(x+1)
ax.text(0.35, 0.95, textstr, transform=ax.transAxes, fontsize=14,
        verticalalignment='top', bbox=props)

autolabel(rects1)
fig.tight_layout()
plt.show()
