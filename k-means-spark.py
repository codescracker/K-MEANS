import numpy as np 
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[*]').setAppName('K-Means')
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")


def getPointsRdd(path):
	lines = sc.textFile(path).map(lambda l : l.split(','))
	points = lines.map(lambda ls : np.array(list(map(float, ls)))).cache()

	return points

def getDistance(p1,p2):
	return sum((p1-p2)**2)


def getClosestCentroid(centroids, p):
	idx = -1
	distance = float('inf')

	for i in range(0,len(centroids)):
		centroid = centroids[i]
		tmpDistance = getDistance(centroid, p)
		if tmpDistance<distance:
			idx = i
			distance = tmpDistance

	return idx


if __name__ == '__main__':
	if len(sys.argv)!=5:
		print('Please input the required arguments: FilePath, Number of cluster, Converged distance, Maxium iteration times')
		exit(-1)

	count = 0
	filePath = sys.argv[1]
	k = int(sys.argv[2])
	threshold = float(sys.argv[3])
	maxIteration = int(sys.argv[4])

	points = getPointsRdd(filePath)
	centroids = points.takeSample(False, k)
	distanceChange = float('inf')

	while distanceChange>threshold and count<maxIteration:
		count+=1
		print('The current threshold is {}, centroids distance change is {}'.format(threshold, distanceChange))
		print('For the iteration {}, the current centroids is {}'.format(count, centroids))

		clusters = points.map(lambda p : (getClosestCentroid(centroids,p),(p,1)))	

		tmpCentroids = clusters.reduceByKey(lambda pair1, pair2 : (pair1[0]+pair2[0], pair1[1]+pair2[1]))\
								.mapValues(lambda pair : pair[0]/pair[1])\
								.collect()

		distanceChange = 0
		for key, newCentroid in tmpCentroids:
			distanceChange+= getDistance(newCentroid, centroids[key])

		for key, newCentroid in tmpCentroids: 
			centroids[key] =newCentroid
	
	print('The distance change for the last iteration is {}'.format(distanceChange))	
	print('The stable centroids are {}'.format(centroids))

			









