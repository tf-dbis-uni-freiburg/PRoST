<p align="center"> <img width="36%" src="PRoST_logo.svg"></p>

## PRoST (Partitioned RDF on Spark Tables)
PRoST allows to load and query very large RDF graphs in Hadoop clusters.
Input graphs are partitioned efficiently and stored across several tables registered in the Hive Metastore. PRoST contains an optimized engine that executes SPARQL queries on its particular data representation. For alternative ad-hoc solutions, a graph can be also queried using common Hadoop technologies, e.g. Spark SQL, Hive or Impala. 

## Publications
  - Cossu, Matteo, Michael Färber, and Georg Lausen. "PRoST: Distributed Execution of SPARQL Queries Using Mixed Partitioning Strategies". (EDBT 2018).
  -	Victor Anthony Arrascue Ayala, Georg Lausen. "A Flexible N-Triples Loader for Hadoop. International Semantic Web Conference". (ISWC 2018).
  - Victor Anthony Arrascue Ayala, Polina Koleva, Anas Alzogbi, Matteo Cossu, Michael Färber, Patrick Philipp, Guilherme Schievelbein, Io Taxidou, Georg Lausen. "Relational Schemata for Distributed SPARQL Query Processing". (SBD@SIGMOD 2019).
  - Guilherme Schievelbein, Victor Anthony Arrascue Ayala, Fang Wei-Kleiner, Georg Lausen. "Exploiting Wide Property Tables Empowered by Inverse Properties for Efficient Distributed SPARQL Query Evaluation". (ISWC 2019).
  
## Support
prost@informatik.uni-freiburg.de

## Requirements
  - Apache Spark 2+
  - Hive
  - HDFS
  - Java 1.8+
  - A local Hadoop environment to be able to run the tests
  
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
First, clone this repository. The project contains two separate components, one for loading the data (prost-loader) and the other for querying (prost-query_executor).
Both are built using [Apache Maven](http://maven.apache.org/).
To build PRoST run on the parent directory:

    mvn package
	
## Running test cases
You need to have a local Hadoop environment. In particular you need to set up the environment variable "HADOOP_HOME" to point to your Hadoop distribution.
Install the same version of Hadoop we are using, which is: hadoop-2.7.1, and set the default jdk version to Java 8.

For Windows users: you need to winutils.exe binary with the above-mentioned distribution of Hadoop.
Further details see here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
Make sure to install the latest version of the VC++ Redistributable and change the writing permissions of the hive temporary folter.

For Linux users: there is also plenty of documentation. 
You can follow instructions from: https://doctuts.readthedocs.io/en/latest/hadoop.html

To run the test cases:

	mvn test


## PRoST-Loader: loading RDF graphs and creating the logical partitions.
PRoST-Loader generates partitions according to the following five strategies: Triple Table (TT), Wide Property Table (WPT), Inverse Wide Property Table (IWPT), Joined Wide Property Table (JWPT), and Vertical Partitioning (VP).

You can load a graph with PRoST in the following way:

    spark2-submit --driver-java-options -ea --class run.Main PRoST-Loader.jar -i <HDFS_path_RDF_graph> -o <output_DB_name>
	Example:
	spark2-submit --driver-java-options -ea --class run.Main /home/user/PRoST-Loader-0.0.1-SNAPSHOT.jar -i /data/original/DbPedia -o dbpedia
	

The input RDF graph is loaded from the HDFS path specified with the -i option.

The option -o contains the name of the database in which PRoST will store the graph using its own representation.

PRoST-loader default settings are defined in the file prost-loader-default.ini. An alternative settings file can be used with the argument -pref <settings_file.ini>.

The datasets can be loaded into the different partitioning strategies as long as the setting dropDB is set to false. When set to true, all pre-existing tables in the database are deleted. Note that a Triple Table (TT) is required to load the data with the other models.

The computation of the property statistics is done on top of the VP tables and is required to execute queries. The computation of characteristic sets is optional.

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

    spark2-submit --driver-java-options -ea --class run.Main PRoST-Query.jar -i <SPARQL_query> -db <DB_name> -o <HDFS_output_file> 
    Example:
	spark2-submit --driver-java-options -ea --class run.Main PRoST-Query.jar -i /DbPedia/q1.txt -db dbpedia -o q1_dbpedia

This command will execute the queries using the combination of the partitioning strategies set in the file prost-query-executor-default.ini. An alternative settings file can be used with the optional argument -pref <settings.ini>.

Any combination of partitioning strategies can be used to execute queries, as long as all triple patterns can be included in a node using one of the enabled partitioning strategies. Optionally, a .cvs file can be created with benchmark information collected during the execution of the queries.

The optional -o option contains the name of the HDFS file in which PRoST will save the results of the query.
