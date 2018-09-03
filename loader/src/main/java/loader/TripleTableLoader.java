package loader;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class that constructs a triples table. First, the loader creates an external
 * table ("raw"). The data is read using SerDe capabilities and by means of a
 * regular expresion. An additional table ("fixed") is created to make sure that
 * only valid triples are passed to the next stages in which other models e.g.
 * Property Table, or Vertical Partitioning are built.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoader extends Loader {
	protected boolean ttPartitionedBySub = false;
	protected boolean ttPartitionedByPred = false;

	public TripleTableLoader(String hdfs_input_directory, String database_name, SparkSession spark, 
			boolean ttPartitionedBySub, boolean ttPartitionedByPred) {
		super(hdfs_input_directory, database_name, spark);
		this.ttPartitionedBySub = ttPartitionedBySub;
		this.ttPartitionedByPred = ttPartitionedByPred;
	}

	@Override
	public void load() throws Exception {
		logger.info("PHASE 1: loading all triples to a generic table...");
		String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		String queryDropTripleTableFixed = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		String createTripleTableFixed = null;
		String repairTripleTableFixed = null;
		
		spark.sql(queryDropTripleTable);
		spark.sql(queryDropTripleTableFixed);

		String createTripleTableRaw = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %1$s(%2$s STRING, %3$s STRING, %4$s STRING) ROW FORMAT SERDE "
						+ "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
						+ "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.+)\\\\s*\\\\.\\\\s*$\")"
						+ "LOCATION '%5$s'",
				name_tripletable + "_ext", column_name_subject, column_name_predicate, column_name_object,
				hdfs_input_directory);
		spark.sql(createTripleTableRaw);
		
		if (!ttPartitionedBySub && !ttPartitionedByPred) {
			createTripleTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING, %4$s STRING) STORED AS PARQUET",
					name_tripletable, column_name_subject, column_name_predicate, column_name_object);			
		}  else if (ttPartitionedBySub){
			createTripleTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%3$s STRING, %4$s STRING) PARTITIONED BY (%2$s STRING) STORED AS PARQUET",
					name_tripletable, column_name_subject, column_name_predicate, column_name_object);	
		} else if (ttPartitionedByPred){
			createTripleTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %4$s STRING) PARTITIONED BY (%3$s STRING) STORED AS PARQUET",
					name_tripletable, column_name_subject, column_name_predicate, column_name_object);	
		}
		spark.sql(createTripleTableFixed);			

		if (!ttPartitionedBySub && !ttPartitionedByPred) {
			repairTripleTableFixed = String.format("INSERT OVERWRITE TABLE %1$s  " 
			+ "SELECT %2$s, %3$s, trim(%4$s)  "
			+ "FROM %5$s " + "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
			+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND "
			+ "NOT(%4$s RLIKE '^\\s*<.*<.*>')  AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\"') AND "
			+ "LENGTH(%3$s) < %6$s" ,
			name_tripletable, column_name_subject, column_name_predicate, column_name_object,
			name_tripletable + "_ext", max_length_col_name);
		}  else if (ttPartitionedBySub) {
			repairTripleTableFixed = String.format("INSERT OVERWRITE TABLE %1$s PARTITION (%2$s) " 
					+ "SELECT %3$s, trim(%4$s), %2$s "
					+ "FROM %5$s " + "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
					+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND "
					+ "NOT(%4$s RLIKE '^\\s*<.*<.*>')  AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\"') AND "
					+ "LENGTH(%3$s) < %6$s" ,
					name_tripletable, column_name_subject, column_name_predicate, column_name_object,
					name_tripletable + "_ext", max_length_col_name);
		} else if (ttPartitionedByPred) {
			repairTripleTableFixed = String.format("INSERT OVERWRITE TABLE %1$s PARTITION (%3$s) " 
					+ "SELECT %2$s, trim(%4$s), %3$s "
					+ "FROM %5$s " + "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
					+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND "
					+ "NOT(%4$s RLIKE '^\\s*<.*<.*>')  AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\"') AND "
					+ "LENGTH(%3$s) < %6$s" ,
					name_tripletable, column_name_subject, column_name_predicate, column_name_object,
					name_tripletable + "_ext", max_length_col_name);
		}
		spark.sql(repairTripleTableFixed);

		logger.info("Created tripletable with: " + createTripleTableRaw);
		logger.info("Cleaned tripletable created with: " + repairTripleTableFixed);

		String queryRawTriples = String.format("SELECT * FROM %s", name_tripletable + "_ext");
		String queryAllTriples = String.format("SELECT * FROM %s", name_tripletable);

		Dataset<Row> allTriples = spark.sql(queryAllTriples);
		Dataset<Row> rawTriples = spark.sql(queryRawTriples);
		if (allTriples.count() == 0) {
			logger.error(
					"Either your HDFS path does not contain any files or no triples were accepted in the given format (nt)");
			logger.error("The program will stop here.");
			throw new Exception("Empty HDFS directory or empty files within.");
		} else
			logger.info("Total number of triples loaded: " + allTriples.count());
		if (rawTriples.count() != allTriples.count()) {
			logger.info("Number of corrupted triples found: " + (rawTriples.count() - allTriples.count()));
		}
		List cleanedList = allTriples.limit(10).collectAsList();
		logger.info("First 10 cleaned triples (less if there are less): " + cleanedList);
		
		//This code is to create a TT partitioned by subject with a fixed number of partiitions. 
		//Run the code with: 
		//Delete after results are there.
		/*
		logger.info("Number of partitions of TT before repartitioning: " + allTriples.rdd().getNumPartitions());
		Dataset<Row> allTriplesPart = allTriples.repartition(50, allTriples.col(column_name_predicate));
		allTriplesPart.write().saveAsTable("tripletable_partBySub_50");
		logger.info("Number of partitions after repartitioning: " + allTriplesPart.rdd().getNumPartitions());	
	
		logger.info("Number of partitions of TT before repartitioning: " + allTriples.rdd().getNumPartitions());
		Dataset<Row> allTriples500 = allTriples.repartition(500, allTriples.col(column_name_subject));
		allTriples500.write().saveAsTable("tripletable_partBySub_500");
		logger.info("Number of partitions after repartitioning: " + allTriples500.rdd().getNumPartitions());		
		
		logger.info("Number of partitions of TT before repartitioning: " + allTriples.rdd().getNumPartitions());
		Dataset<Row> allTriples100 = allTriples.repartition(100, allTriples.col(column_name_subject));
		allTriples100.write().saveAsTable("tripletable_partBySub_100");
		logger.info("Number of partitions after repartitioning: " + allTriples100.rdd().getNumPartitions());
		
		logger.info("Number of partitions of TT before repartitioning: " + allTriples.rdd().getNumPartitions());
		Dataset<Row> allTriples25 = allTriples.repartition(25, allTriples.col(column_name_subject));
		allTriples25.write().saveAsTable("tripletable_partBySub_25");
		logger.info("Number of partitions after repartitioning: " + allTriples25.rdd().getNumPartitions());
		
		logger.info("Number of partitions of TT before repartitioning: " + allTriples.rdd().getNumPartitions());
		Dataset<Row> allTriples10 = allTriples.repartition(10, allTriples.col(column_name_subject));
		allTriples10.write().saveAsTable("tripletable_partBySub_10");
		logger.info("Number of partitions after repartitioning: " + allTriples10.rdd().getNumPartitions());			
		*/
	}
}
