__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import sys
import os
import radical.pilot as rp
import time
import copy

SHARED_INPUT_FILE = 'dataset.in'
MY_STAGING_AREA = 'staging:///'

""" DESCRIPTION:  k-means
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

    args = sys.argv[1:]
    if len(args) < 1:
        print "Usage: Give the number of the divisions you want to create Try:"
        print "python k-means k"
        sys.exit(-1)
    k = int(sys.argv[1])  # number of the divisions - clusters

    # Check if the dataset exists  and count the total number of lines of the dataset
    try:
    	data = open(SHARED_INPUT_FILE,'r')
    except IOError:
    	print "Missing data-set. file! Check the name of the dataset"
    	sys.exit(-1)
    total_file_lines =  sum(1 for _ in data) 

	#-----------------------------------------------------------------------
    #Choose randomly k elements from the dataset as centroids
    data.seek(0,0) # move fd to the beginning of the file
    centroid = list()
    for i in range(0,k):
        centroid.append(data.readline())
    data.close()
    centroid =  map(float,centroid)        
    print centroid
    #--------------------------------------------------------------------------
    ## Put the centroids into a file to share
    centroid_to_string = ','.join(map(str,centroid))
    centroid_file = open('centroids.data', 'w')     
    centroid_file.write(centroid_to_string)
    centroid_file.close()   

    #-------------------------------------------------------------------------
    # Initialization of variables
    CUs = 2   # NOTE: Define how many CUs you are willing to use 
    convergence = False   # We have no convergence yet
    m = 0 # number of iterations
    maxIt = 10 # the maximum number of iteration
    chunk_size = total_file_lines/CUs  # this is the size of the part that each unit is going to control    

    #------------------------
    try:
        start_time = time.time()
        #DBURL = "mongodb://localhost:27017"
        session = rp.Session() 

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
        pdesc.cores    = CUs  # define cores 
        pdesc.cleanup  = False
        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)
        #-----------------------------------------------------------------------
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
    
        #-------------------------------------------------------------------------
        ## CUS & map - reduce

        while m<maxIt and False == convergence:
            ## MAPPER PHASE
            mylist = []
            for i in range(1,CUs+1):
                cudesc = rp.ComputeUnitDescription()
                cudesc.executable = "python"
                cudesc.arguments = ['mapper.py', i, k, chunk_size, CUs]
                cudesc.input_staging = ['mapper.py', sd_shared, 'centroids.data']
                cudesc.output_staging = ["combiner_file_%d.data" % i]
                mylist.append(cudesc)
                
            print 'Submitting the CU to the Unit Manager...'
            mylist_units = umgr.submit_units(mylist)
            # wait for all units to finish
            umgr.wait_units()
            print "All Compute Units completed successfully!"
            #-------------------------------------------------------------------------------
            # Aggregate all partial sums of each Cluster  to define the new centroids
            afile = []
            total_sums = []  # total partial sums per cluster
            total_nums = []  # total number of sample samples per cluster
            new_centroids = list()

            # initiate values
            for i in range(0,k):
                total_nums.append(0)
                total_sums.append(0)
                new_centroids.append(0)

            for i in range(0,CUs):
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
                if total_nums[i]!=0:
                    new_centroids[i] =  total_sums[i] / total_nums[i]
            centroid.sort()

            # sort total_nums based on new_centroids list
            zipped = zip(new_centroids, total_nums)
            sorted_zipped = sorted(zipped)
            new_centroids = [point[0] for point in sorted_zipped]
            total_nums = [point[1] for point in sorted_zipped]

            # check convergence and update centroids
            for i in range(0,k):
                if total_nums[i]!=0 and abs(centroid[i] - new_centroids[i])>=0.1*centroid[i]:
                    convergence = False
                    if total_nums[i]!=0:
                        centroid[i] = new_centroids[i]

            # Put the centroids into a file to share
            centroid_to_string = ','.join(map(str,centroid))
            centroid_file = open('centroids.data', 'w')
            centroid_file.write(centroid_to_string)
            centroid_file.close()
            #--------------------------------------------------------------------------------


        #--------------------END OF K-MEANS ALGORITHM --------------------------#
        # K - MEANS ended successfully - print total times and centroids
        print 'K-means algorithm ended successfully after %d iterations' % m
        total_time = time.time() - start_time  # total execution time
        print 'The total execution time is: %f seconds' % total_time
        total_time /= 60
        print 'Which is: %f minutes' % total_time
        print 'Centroids:'
        print centroid

        #cleanup intermediate files
        for i in range(1,CUs+1):
            string = 'combiner_file_%d.data' % i
            os.remove(string)

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "Session closed, exiting now ..."
        session.close(cleanup=True, terminate=True)






