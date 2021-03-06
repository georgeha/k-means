import sys, random 
import matplotlib.pyplot as plt


args = sys.argv[1:]
if len(args) < 1:
    print "Usage: python %s needs the  number of the dataset. Please run again" % __file__
    print "python create_dataset.py n"
    sys.exit(-1)

n = int(sys.argv[1])  # k is the number of clusters i want to create
# read the dataset from dataset.data f

#open the file where we write the results
data_file = open('dataset.in', 'w')

DIMENSIONS = 3


#coordinates = list()
for i in range(0,DIMENSIONS*n):
    coordinates = random.expovariate(1)*100
    data_file.write(str(coordinates))
    data_file.write("\n")
data_file.close()

"""
plt.plot(coordinates)
plt.ylabel('Expovariate Numbers')
plt.show()

"""

