__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The Pilot Program"
__license__ = "MIT"

import sys
import gzip

def get_distance(dataPointX, centroidX):
    # Calculate Euclidean distance.
    return abs(centroidX - dataPointX)
# ------------------------------------------------------------------------------
#

################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]

    cu = int(sys.argv[1])
    k = int(sys.argv[2])  #number of clusters
    part_length = int(sys.argv[3])
    cores = int(sys.argv[4])
    #print 'cu: %d, k: %d, part_length: %d, cores %d' % (cu, k, part_length, cores)

    #----------------------Reading the Centroid files-------------------------------
    centroid = []
    data = open("centroids.data", "r")
    read_as_string_array = data.readline().split(',')
    centroid = map(float, read_as_string_array)
    data.close()
    #-----------------Reading the Elements file --------------
    elements = []
    read_file = gzip.open('dataset.gz', 'rb')
    read_as_string_array = read_file.readline().split(',')
    elements = map(float, read_as_string_array)

    start_part = (cu-1)*part_length
    end_part = (cu)*(part_length)-1
    if cu == cores:
        end_part = len(elements)  # in case it is not equally divided make sure that the last CU checks until the last element of the list
    #---------------------------------------------------------------------------------
    # Open files to append the results of the mapper
    mapper_res = open("mapper_res_%d.data" % cu, "a")
    #----------------------------------------------------------------------------------
    # Mapper
    #Map function
    for i in range(start_part,end_part):
        minDist = get_distance(elements[i], centroid[0])
        cluster = 0
        for j in range(1,k):
            curDist = get_distance(elements[i], centroid[j])
            if minDist != min(minDist,curDist):
                cluster = j  # closest centroid is centroid No: j
                minDist = curDist
        string = '%s\t%s\n' % (cluster, elements[i])   #mapper prints the key (=index) & the value (=elements[i])
        print string
        mapper_res.write(string)

    mapper_res.close()
    #-----------------------------------------------------------------------------------
    # Combiner
    mapper_res = open("mapper_res_%d.data" % cu, "rb")

    sum_elements_per_centroid = []  # partial sum of cluster's sample in the same map task
    num_elements_per_centroid = []  # here we record the number of samples in the same cluster in the same map task.
    for i in range(0,k):
        sum_elements_per_centroid.append(0)
        num_elements_per_centroid.append(0)

    # Combine function  - In the combine function, we partially sum the values of the points assigned to
    #the same cluster.
    for line in mapper_res:
        line = line.strip()  # remove the newline character
        cluster, value = line.split('\t', 2) # split index and count to numbers
        cluster = int(cluster)
        sum_elements_per_centroid[cluster] += float(value)
        num_elements_per_centroid[cluster] += 1
    #close the mapper_file
    mapper_res.close()

    # Write the results of the combiner function to a file for the reducer function
    combiner_file = open("combiner_file_%d.data" % cu, "a")
    for cluster in range(0,k):
        string = '%s\t%s\t%s\n' % (cluster, sum_elements_per_centroid[cluster],num_elements_per_centroid[cluster])
        combiner_file.write(string) ## key = cluster value_1 = partial sum of cluster samples
    combiner_file.close()





