__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import sys

def get_distance(dataPointX, centroidX):
    # Calculate Euclidean distance.
    return abs(centroidX - dataPointX)
# ------------------------------------------------------------------------------
#

################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]

    curent_cu = int(sys.argv[1])
    k = int(sys.argv[2])  #number of clusters
    chunk_size = int(sys.argv[3])
    total_CUs = int(sys.argv[4])

    #----------------------Reading the Centroid files-------------------------------
    centroid = list()
    data = open("centroids.data", "r")   
    read_as_string_array = data.readline().split(',')
    centroid = map(float, read_as_string_array)
    data.close()

    #-----------------Reading the Elements file --------------
    read_file = open('dataset.in', 'rb')
    start_reading_from = (curent_cu-1)*chunk_size
    stop_reading_at = (curent_cu)*(chunk_size)-1

    if curent_cu == total_CUs:
        stop_reading_at = "" # this is the sign of EOF. last CU compute until the EOF

    # we only read the elements we will process - Each CU compute a part of the file
    elements = list()
    for i, line in enumerate(read_file):
        if i == "" or i>stop_reading_at:
            break
        if i >= start_reading_from and i<=stop_reading_at:
            elements.append(line)
    read_file.close()
    elements = map(float, elements)
    
    #----------------------------------------------------------------------------------
    sum_elements_per_centroid = list()  # partial sum of cluster's sample in this task
    num_elements_per_centroid = list()  # number of samples of each cluster in the same map task.
    for i in range(0,k):
        sum_elements_per_centroid.append(0)
        num_elements_per_centroid.append(0)

    for i in range(0,len(elements)):
        minDist = get_distance(elements[i], centroid[0])
        cluster = 0
        for j in range(1,k):
            curDist = get_distance(elements[i], centroid[j])
            if minDist != min(minDist,curDist):
                cluster = j  # closest centroid is centroid No: j
                minDist = curDist
        sum_elements_per_centroid[cluster] += elements[i]
        num_elements_per_centroid[cluster] += 1
        


    # Write results into a file
    combiner_file = open("combiner_file_%d.data" % curent_cu, "w")
    for cluster in range(0,k):
        string = '%s\t%s\t%s\n' % (cluster, sum_elements_per_centroid[cluster],num_elements_per_centroid[cluster])
        combiner_file.write(string) 
    combiner_file.close()




