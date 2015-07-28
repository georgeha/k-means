__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The Pilot Program"
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
        if i == "":
            break
        if i >= start_reading_from and i<=stop_reading_at:
            elements.append(line)
    read_file.close()
    elements = map(float, elements)
    
    #---------------------------------------------------------------------------------
    # Open files to append the results of the mapper
    mapper_res = open("mapper_res_%d.data" % curent_cu, "w")   # Note to self: itane append mode...mln prp na einai se w omws
    #----------------------------------------------------------------------------------
    # Mapper
    #Map function
    for i in range(0,len(elements)):
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
    mapper_res = open("mapper_res_%d.data" % curent_cu, "rb")

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
    mapper_res.close()

    # Write the results of the combiner function to a file for the reducer function
    combiner_file = open("combiner_file_%d.data" % curent_cu, "w")
    for cluster in range(0,k):
        string = '%s\t%s\t%s\n' % (cluster, sum_elements_per_centroid[cluster],num_elements_per_centroid[cluster])
        combiner_file.write(string) ## key = cluster value_1 = partial sum of cluster samples
    combiner_file.close()




