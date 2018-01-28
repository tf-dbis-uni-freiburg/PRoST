<p align="center"> <img width="36%" src="PRoST_logo.svg"></p>

## PRoST (Partitioned RDF on Spark Tables)
PRoST allows to load and query very large RDF graphs in Hadoop clusters.
Input graphs are partitioned efficiently and stored across several tables registered in the Hive Metastore. PRoST contains an optimized engine that executes SPARQL queries on its particular data representation. For alternative ad hoc solutions, a graph can be also queried using common Hadoop technologies, e.g Spark SQL, Hive or Impala. 

## Requirements
  - Apache Spark 2+
  - Hive
  - HDFS
  - Java 1.8+

## Getting the code and compiling
First, clone this repository. The project contains two separate components, one for loading the data (/loader) and the other for querying(/query).
Both are built using [Apache Maven](http://maven.apache.org/).
To build PRoST, run:

    mvn package

## Loading RDF graphs
You can load a graph with PRoST in the following way:

    spark2-submit --class run.Main PRoST-Loader.jar -i <HDFS_path_RDF_graph> -o <output_DB_name> -s

The input RDF graph is loaded from the HDFS path specified with the -i option.
Instead, the -o option contains the name of the database in which PRoST will store the graph using its own representation.

If the option -s is present, the loader produces a .stats file in the local node, required for querying.

## Querying with SPARQL
To query the data use the following command:

    spark2-submit --class run.Main PRoST-Query.jar -i <SPARQL_query> -d <DB_name> -s <stats_file> -o <HDFS_output_file>
    
The database name and the statistics file need to be the ones used to load the graph.
The -o option contains the name of the HDFS file in which PRoST will save the results of the query.
