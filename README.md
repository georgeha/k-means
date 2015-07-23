Welcome to k-means algorithm documentation!
===========================================
| This algorithm implements the k-means algorithm using the RADICAL PILOT API.
| RADICAL-Pilot needs Python >= 2.6. All dependencies are installed automatically by the installer. Besides that, RADICAL-Pilot needs access to a MongoDB database that is reachable from the internet. User groups within the same institution or project can share a single MongoDB instance.
|
Hands-on Job Sumbimission:
^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to make this example work, we need first to  install the following:::

	virtualenv $HOME/myenv
	source $HOME/myenv/bin/activate

Install Radical-Pilot API::
	
	pip install radical.pilot:


Install MondoDB (only if you want to run locally):

	Linux Users::

		apt-get -y install scons libssl-dev libboost-filesystem-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev
		git clone -b r2.6.3 https://github.com/mongodb/mongo.git
		cd mongo
		scons --64 --ssl all
		scons --64 --ssl --prefix=/usr install

	Mac Users::

		brew install mongodb
		mkdir -p /data/db
		chmod 755 /data/db
		mongod


Finally, you need to download the source files of k-means algorithm::


Run the Code:
^^^^^^^^^^^^^

To give it a test drive try via command line the following command::
	
	python k-means.py X

where X is the number of clusters the user wants to create.



More About this algorithm:
^^^^^^^^^^^^^^^^^^^^^^^^^^

This algorithm creates the clusters of the elements found in the dataset.in file. You can create your own file 
or create a new dataset file using the following generator::
	
	curl -O https://raw.githubusercontent.com/georgeha/k-means-algorithm/master/creating_dataset.py

run via command line::

	python creating_dataset.py (number_of_elements)