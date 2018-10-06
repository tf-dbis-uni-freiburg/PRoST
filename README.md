<p align="center"> <img width="36%" src="PRoST_logo.svg"></p>

## PRoST (Partitioned RDF on Spark Tables)
PRoST allows to load and query very large RDF graphs in Hadoop clusters.
Input graphs are partitioned efficiently and stored across several tables registered in the Hive Metastore. PRoST contains an optimized engine that executes SPARQL queries on its particular data representation. For alternative ad-hoc solutions, a graph can be also queried using common Hadoop technologies, e.g. Spark SQL, Hive or Impala. 

## Publications
  - Cossu, Matteo, Michael Färber, and Georg Lausen. "PRoST: Distributed Execution of SPARQL Queries Using Mixed Partitioning Strategies." (EDBT 2018).
  
## Support
prost@informatik.uni-freiburg.de

## Requirements
  - Apache Spark 2+
  - Hive
  - HDFS
  - Java 1.8+
  
## Recommended cluster settings (relevant for the loader)
The setting names are specific to Cloudera 5.10.0.
  - spark.executor.memory: > 20 GB 
  - spark.yarn.executor.memoryOverhead: > 8 GB
  - spark.driver.memory: > 16 GB
  - spark.yarn.driver.memoryOverhead > 16 GB
  - spark.yarn.executor.memoryOverhead > 8 GB
  - yarn.scheduler.minimum-allocation-mb >  4 GB 
  - yarn.nodemanager.resource.memory-mb > 22 GB
 

## Getting the code and compiling
First, clone this repository. The project contains two separate components, one for loading the data (/loader) and the other for querying (/query).
Both are built using [Apache Maven](http://maven.apache.org/).
To build PRoST run:

    mvn package

## PRoST-Loader: loading RDF graphs and creating the logical partitions.
PRoST-Loader generates partitions according to the following three strategies: Triple Table (TT), Wide Property Table (WPT), and Vertical Partitioning (VP).

NEW: Support for N-Triples documents.

NEW: Added an option "lp" to specify a logical partition strategy.

You can load a graph with PRoST in the following way:

    spark2-submit --class run.Main PRoST-Loader.jar -i <HDFS_path_RDF_graph> -o <output_DB_name> -lp <logical_partition_strategies> -s
	Example:
	spark2-submit --class run.Main /home/user/PRoST-Loader-0.0.1-SNAPSHOT.jar -i /data/original/DbPedia -o dbpedia -lp TT,WPT,VP -s
	

The input RDF graph is loaded from the HDFS path specified with the -i option.

The option -o contains the name of the database in which PRoST will store the graph using its own representation.

The option -lp allows one to specify a logical partitioning strategy. The argument is a comma-separated list of strategies, for instance "TT,WPT,VP".
Possible values are "TT" for triple table, "WPT" for Wide Property Table, "IPT" for Inverse Wide Property Table, "JPT" for a single table with the joined data of both Inverse Wide Property Table and Wide Property Table, and "VP" for Vertical Partitioning. If this option is missing, the default is "TT,WPT,VP".
Note that you should not include spaces for multiple strategies, otherwise the program will consider only the first strategy. 
The strategy "TT" might be automatically triggered if "WPT", "IPT", "JPT" or "VP" are selected because they have a dependency on that table.

If the option -s is present, the loader produces a .stats file in the local node, required for querying.
Note that this file will be generated in the same path from which the application is run. 

Please be aware that there might be limitations in the number of columns a wide property table might have in order to be written.
We have successfully tested our approach on approx. 1500 columns without problems.

PRoST-Loader defines its own logger (Logger.getLogger("PRoST")) and uses it to log all relevant information related to loading and partitioning the dataset. If no actions are taken, the messages will end up in Spark's logger.
You can modify Spark's log4j.properties to forward the messages to a different place, e.g. a file.
If you wish to do so, add the following lines to the log4j.properties file:

	log4j.logger.PRoST=INFO, fileAppender
	log4j.additivity.PRoST=false
	log4j.appender.fileAppender=org.apache.log4j.RollingFileAppender
	log4j.appender.fileAppender.File=/var/log/PRoST/PRoST-loader.log
	log4j.appender.fileAppender.MaxFileSize=100MB
	log4j.appender.fileAppender.MaxBackupIndex=1
	log4j.appender.fileAppender.layout=org.apache.log4j.PatternLayout
	log4j.appender.fileAppender.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1} - %m%n


## PRoST-Query: Querying with SPARQL
To query the data use the following command:

    spark2-submit --class run.Main PRoST-Query.jar -i <SPARQL_query> -d <DB_name> -s <stats_file> -o <HDFS_output_file>
    
The database name and the statistics file need to be the ones used to load the graph.
The -o option contains the name of the HDFS file in which PRoST will save the results of the query.
