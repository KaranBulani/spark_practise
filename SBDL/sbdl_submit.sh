spark-submit --master yarn --deploy-mode cluster \
--py-files sdbl_lib.zip \
--files conf/sdbl.conf,conf/spark.conf,log4j.properties \
--driver-cores 2 \
--driver-memory 3G \
--conf spark.driver.memoryOverhead=1G
sbdl_main.py qa 2022-08-02
: '
spark-submit
	The command used to submit Spark applications to a cluster.
--master yarn
	Specifies the cluster manager (YARN in this case).
--deploy-mode cluster
	Specifies that the driver program will run on a node in the cluster (instead of on the machine submitting the job). This is good for production jobs because it reduces the load on the submitting machine.
--py-files sdbl_lib.zip
	Distributes additional Python files/dependencies to all worker nodes. sdbl_lib.zip contains shared Python code
--files conf/sdbl.conf,conf/spark.conf,log4j.properties
	This makes additional configuration files available to the Spark job.
	sdbl.conf → Likely contains job-specific configurations.
	spark.conf → Contains Spark-related configurations.
	log4j.properties → Used for logging configuration.
DRIVER CONFIGS
	--driver-cores 2 The spark.driver.cores configuration is given as
	--driver-memory 3G The spark.driver.memory is given as
	--conf spark.driver.memoryOverhead=1G The spark.driver.memoryOverhead is given as
sbdl_main.py qa 2022-08-02
	sbdl_main.py → The main Python script that Spark will execute.
	qa → A command-line argument (could specify an environment like QA, prod, or dev).
	2022-08-02 → Another command-line argument (likely a date, possibly for processing data from that date).
'