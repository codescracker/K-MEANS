from __future__ import print_function

import numpy as np 
import random
import sys

def getDistance(p1,p2):
	return sum((p1-p2)**2)


def closestCentroid(centroids, p):
	distance = float('inf')
	index = -1
	for i in range(0,len(centroids)):
		centroid = centroids[i]
		tempDis = getDistance(centroid,p)
		if tempDis<distance:
			index = i
			distance = tempDis

	return index


def getPoints(path):
	with open(path,'r') as f: 
		res = []
		for line in f:
			tmp = map(float,line.split(','))
			res.append(np.array(tmp))

	return res 

def getInitialCentroids(n,dataset):
	ls = [i for i in range(0,len(dataset))]
	sample = random.sample(ls,n)

	return [dataset[idx] for idx in sample]

def getNewCentroid(cluster):
	return sum(cluster)/len(cluster)

if __name__ == '__main__':
	if len(sys.argv)!=5:
		print('Please input all required arguments')
		exit(-1)

	count = 0	
	filePath = sys.argv[1]
	k = int(sys.argv[2])
	threshold = float(sys.argv[3])
	maxIteration = int(sys.argv[4])

	dataset = getPoints(filePath)
	centroids = getInitialCentroids(k, dataset)
	distanceChange = float('inf')

	while distanceChange > threshold and count<maxIteration:
		count+=1
		print('Iteration {}\'s centroids are {}'.format(count, centroids))
		print('Current distanceChange is {}, threshold is {}'.format(distanceChange, threshold))
		
		clusters = {i:[] for i in range(0,k)}

		for p in dataset:
			cluster = closestCentroid(centroids, p)
			clusters[cluster].append(p)

		distanceChange = 0
		for key in clusters:
			pointes = clusters[key]
			newcentroid = sum(pointes)/len(pointes)
			distanceChange+= getDistance(centroids[key], newcentroid) 
			centroids[key] = newcentroid

	print('The distance change for the last iteration is {}'.format(distanceChange))
	print('Stable centroids is {}'.format(centroids))











	



