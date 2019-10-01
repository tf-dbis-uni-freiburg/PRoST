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
First, clone this repository. The *main* branch contains the latest stable version of PRoST. PRoST consists of multiple modules, which are built using [Apache Maven](http://maven.apache.org/). To build all modules, run the following command on the parent directory:

    mvn package
	
To skip running tests when building a module, add the following argument when building it:
	
	-DskipTests
	
## Running test cases
You need to have a local Hadoop environment. In particular, you need to set up the environment variable "HADOOP_HOME" to point to your Hadoop distribution.
Install the same version of Hadoop we are using, which is: hadoop-2.7.1, and set the default JDK version to Java 8.

**For Windows users:** you need to winutils.exe binary with the above-mentioned distribution of Hadoop.
Further details see here: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html.
Make sure to install the latest version of the VC++ Redistributable and change the writing permissions of the hive temporary folder.

**For Linux users:** there is also plenty of documentation. 
You can follow instructions from https://doctuts.readthedocs.io/en/latest/hadoop.html

To run the test cases, execute the following command:

	mvn test
	
#PRoST logger
PRoST-Loader and PRoST-Query_Executor define its own logger (Logger.getLogger("PRoST")) and uses it to log all relevant information related to loading and partitioning the dataset, as well as executing SPARQL queries. If no actions are taken, the messages will end up in Spark's logger.
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


## PRoST-Loader: loading RDF graphs and creating logical partitions.
PRoST-loader is the module used to load graph data into an Apache Hive database. PRoST can take advantage of data replication on multiple database designs for improving query performance. 

PRoST can use any of the following database designs: Triple Table (TT), Vertical Partitioning (VP), Wide Property Table (WPT), Inverse Wide Property Table (iWPT), and three variations of Joined Wide Property Tables (jWPT).

A graph can be loaded with PRoST with the following command:

    spark-submit --driver-java-options -ea --class run.Main prost-loader.jar -db <output_DB_name> -i <HDFS_path_to_RDF_graph>
	Example:
	spark-submit --driver-java-options -ea --class run.Main prost-loader.jar -db dbpedia  -i /data/original/DbPedia
	
The HDFS path to the RDF graph is only required to create the Triple Table (TT). The other tables are created from an existing Triple table in the database.

The parameter **-i** with the path in HDFS to the RDF graph is only required when the database does not exist or does not contain a Triples Table of the graph.

PRoST-loader default settings are defined in the file *prost-loader-default.ini*. An alternative settings file can be used with the argument *-pref <settings_file.ini>*.

The datasets can be loaded into the different partitioning strategies as long as the setting dropDB is set to false. When set to true, all pre-existing tables in the database are deleted. Note that a Triple Table (TT) is required to load the data with the other models.

The computation of the property statistics is done on top of the VP tables and is required to execute queries. The computation of characteristic sets is optional.

Please be aware that there might be limitations on the number of columns a wide property table might have in order to be written.
We have successfully tested our approach on approx. 1500 columns without problems.

## PRoST-Query: Querying with SPARQL
PRoST-query_executor is the module used to execute SPARQL queries on databases generate by PRoST-loader.

To query the data use the following command:

    spark2-submit --driver-java-options -ea --class run.Main PRoST-Query.jar -i <SPARQL_query> -db <DB_name> 
	Example:
	spark2-submit --driver-java-options -ea --class run.Main PRoST-Query.jar -i /DbPedia/queries/ -db dbpedia

This command will execute the queries using the settings defined in the file prost-query-executor-default.ini. An alternative settings file can be used with the optional argument -pref <settings.ini>.

The input -i may be either a folder containing the SPARQL queries files or a single query file.

Optionally, the queries results may be saved to HDFS with the argument -o <HDFs_output_folder_name>.