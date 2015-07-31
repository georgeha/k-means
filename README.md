Welcome to k-means algorithm documentation!
===========================================

Installation:
First install Radical-Pilot and its dependences. You can find out how to do it 
here:  http://radicalpilot.readthedocs.org/en/latest/installation.html#id1

 
 Download source code::

	curl -O https://raw.githubusercontent.com/georgeha/k-means/master/k-means.py
	curl -O https://raw.githubusercontent.com/georgeha/k-means/master/mapper.py

Script to create randomly dataset:

	curl -O https://raw.githubusercontent.com/georgeha/k-means/master/creating_dataset.py

Run via command line to create a dataset:

	python creating_dataset.py (number_of_elements)


If you are using your own dataset:
The name of the input file must be dataset.in , and the elements must be written one at each line.

Run the Code:

To give it a test drive try via command line the following command::
	
	python k-means.py X

where X is the number of clusters the user wants to create.


More info:

This algorithm uses a local MongoDB database. You can export your own database using export.

	Line 80: You can choose the number of Compute Units you are creating. 
	I believe the optimum is to be equal with cores


 Algorithm Implementation:

 This algorithm is stage in to pilot the input dataset. It chooses the first k elements of the dataset randomly 
 as the initial centroids. Then, it creates n tasks and each task is computing the partial sums of the elements
 of each cluster and save them to a file. Each of the task is computing part of the dataset which is equally 
 divided to them. Locally, it aggregates all partial sums of each Cluster  to define the new centroids. If there
 is a convergence the algorithm stops  . If there is no convergence after 10 loops the algorithm ends to prevent 
 from getting into a local minimum forever. Finally it saves  the centroids to: centroids.data
