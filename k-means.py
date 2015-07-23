__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The Pilot Program"
__license__ = "MIT"

import sys
import os
import radical.pilot as rp
import saga
import time
import gzip
import copy


SHARED_INPUT_FILE = 'dataset.gz'
MY_STAGING_AREA = 'staging:///'

""" DESCRIPTION:  mpk-means
For every task A_n (mapper)  is started
"""

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scenes!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state) :
    """ this callback is invoked on all pilot state changes """

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if  state == rp.FAILED :
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state) :
    """ this callback is invoked on all unit state changes """

    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new session. A session is the 'root' object for all other
        # RADICAL-Pilot objects. It encapsulates the MongoDB connection(s) as
        # well as security contexts.
        DBURL = "mongodb://localhost:27017"
        session = rp.Session(database_url = DBURL)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        # Change the user name below if you are using a remote resource 
        # and your username on that resource is different from the username 
        # on your local machine. 
        #
        #c = rp.Context('ssh')
        #c.user_id = "username"
        #c.user_pass = "passcode"
        #session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        # If you want to run this example on your local machine, you don't have 
        # to change anything here. 
        # 
        # Change the resource below if you want to run on a remote resource. 
        # You also might have to set the 'project' to your allocation ID if 
        # your remote resource does compute time accounting. 
        #
        # A list of preconfigured resources can be found at: 
        # http://radicalpilot.readthedocs.org/en/latest/machconf.html#preconfigured-resources
        # 
        # define the resources you need
        pdesc = rp.ComputePilotDescription()
        pdesc.resource =  "local.localhost"  # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 10 # minutes
        pdesc.cores    = 2
        pdesc.cleanup  = True

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # Read the number of the divisions you want to create
        args = sys.argv[1:]
        if len(args) < 1:
            print "Usage: Give the number of the divisions you want to create Try:" % __file__
            print "python k-means k"
            sys.exit(-1)
        k = int(sys.argv[1])  # number of the divisions - clusters
        #-----------------------------------------------------------------------
        read_and_tarball_time = time.time()
        # Read the dataset from a local file and  and create a tarball to make it smaller
        try:
            data = open("dataset.in",'r')
        except IOError:
            print "Missing data-set. file! Check the name of the data-set"
            sys.exit(-1)

        dataset_as_string_array = data.readline().split(',')
        data.close()
        x = map(float, dataset_as_string_array)
        tarball_string = ','.join(map(str,x))


        # Create a tarball for the dataset to save time & space
        f = gzip.open('dataset.gz', 'wb')
        f.write(tarball_string)
        f.close()
        read_and_tarball_time = read_and_tarball_time - time.time()
        print 'read and tarball time = %d' % read_and_tarball_time

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE)
        staged_file = "%s%s" % (MY_STAGING_AREA, SHARED_INPUT_FILE)

        # Configure the staging directive for to insert the shared file into
        # the pilot staging directory.
        sd_pilot = {'source': shared_input_file_url,
                    'target': staged_file,
                    'action': rp.TRANSFER
        }
        # Synchronously stage the data to the pilot
        pilot.stage_in(sd_pilot)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': staged_file, 
                     'target': SHARED_INPUT_FILE,
                     'action': rp.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager(session, rp.SCHED_DIRECT_SUBMISSION)
        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        #------------------------------------------------------------------------
        # Choosing centroids
        ## The centroids selection is done randomly - so we choose the first k elements, for less computations
        centroid = []
        for i in range(0,k):
            centroid.append(x[i])
        print centroid
       #--------------------------------------------------------------------------
       ## Put the centroids into a file to share
        centroid_to_string = ','.join(map(str,centroid))
        centroid_file = open('centroids.data', 'w')     
        centroid_file.write(centroid_to_string)
        centroid_file.close()       
        #-------------------------------------------------------------------------
        # Initialization of variables
        convergence = False   # We have no convergence yet
        m = 0 # number of iterations
        maxIt = 10 # the maximum number of iteration
        part_length = len(x)/pdesc.cores  # this is the length of the part that each unit is going to control
        #-------------------------------------------------------------------------
        ## CUS & map - reduce
        start_time = time.time() # start map - reduce timer

        while m<maxIt and False == convergence:
            ## MAPPER PHASE
            mylist = []
            for i in range(1,pdesc.cores+1):
                cudesc = rp.ComputeUnitDescription()
                cudesc.environment = {"cu": "%d" % i, "k": "%d" % k, "part_length": "%d" % part_length, "cores": "%d" % pdesc.cores}
                cudesc.executable = "python"
                cudesc.arguments = ['mapper.py', '$cu','$k', '$part_length', '$cores']
                cudesc.input_staging = ['mapper.py', sd_shared, 'centroids.data']
                cudesc.output_staging = []
                file_name = "combiner_file_%d.data" % i
                cudesc.output_staging.append(file_name)
                mylist.append(cudesc)
                
            print 'Submitting the CU to the Unit Manager...'
            mylist_units = umgr.submit_units(mylist)
            # wait for all units to finish
            umgr.wait_units()
            print "All Compute Units completed PhaseA successfully! Now.."
            #-------------------------------------------------------------------------------
            # REDUCER - The input of the reduce function is the data obtained from the combine function of each host.
            # In reduce function, we can sum all the samples and compute the total number of samples assigned to the same cluster, to find the new centroids
            afile = []
            total_sums = []  # total partial sums per cluster
            total_nums = []  # total number of sample samples per cluster
            # initiate values
            for i in range(0,k):
                total_nums.append(0)
                total_sums.append(0)

            for i in range(0,pdesc.cores):
                afile.append(open("combiner_file_%d.data" % (i+1), "rb"))
                for line in afile[i]:
                    line = line.strip()  #remove newline character
                    cluster,p_sum,num = line.split('\t',3)   # split line into cluster No, partial sum and number of partial sums
                    cluster = int(cluster)
                    total_sums[cluster] += float(p_sum)
                    total_nums[cluster] += int(num)
                afile[i].close()
            # new values
            convergence = True
            m+=1
            for i in range(0,k):

                if total_nums[i]!=0 and centroid[i] != total_sums[i] / total_nums[i]:
                    convergence = False
                    if total_nums[i]!=0:
                        centroid[i] = total_sums[i] / total_nums[i]

            # Put the centroids into a file to share
            centroid_to_string = ','.join(map(str,centroid))
            centroid_file = open('centroids.data', 'w')
            centroid_file.write(centroid_to_string)
            centroid_file.close()
            #--------------------------------------------------------------------------------


        #--------------------END OF K-MEANS ALGORITHM --------------------------#
        # K - MEANS ended successfully - print total times and centroids
        print 'MR-K-means algorithm ended successfully after %d iterations' % m
        total_time = time.time() - start_time  # total execution time
        print 'The total execution time is: %f seconds' % total_time
        total_time /= 60
        print 'Which is: %f minutes' % total_time
        print 'Centroids:'
        print centroid
        session.close()
        print "Session closed, exiting now ..."

        #cleanup intermediate files
        for i in range(1,pdesc.cores+1):
            string = 'combiner_file_%d.data' % i
            os.remove(string)

        sys.exit(0)

    except Exception, e:
        print "AN ERROR OCCURRED: %s" % (str(e))
        sys.exit(-1)






