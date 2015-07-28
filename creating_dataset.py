import sys, random 


args = sys.argv[1:]
if len(args) < 1:
    print "Usage: python %s needs the  number of the dataset. Please run again" % __file__
    print "python create_dataset.py n"
    sys.exit(-1)

n = int(sys.argv[1])  # k is the number of clusters i want to create
# read the dataset from dataset.data f

#open the file where we write the results
data_file = open('dataset.in', 'w')


for i in range(0,n):
    a = random.uniform(0, 100000)
    data_file.write(str(a))
    data_file.write("\n")

data_file.close()


