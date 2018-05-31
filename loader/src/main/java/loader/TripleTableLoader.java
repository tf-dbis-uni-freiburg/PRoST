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

	public TripleTableLoader(String hdfs_input_directory, String database_name, SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}

	@Override
	public void load() throws Exception {
		logger.info("PHASE 1: loading all triples to a generic table...");
		String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		String queryDropTripleTableFixed = String.format("DROP TABLE IF EXISTS %s", name_tripletable + "_fixed");

		spark.sql(queryDropTripleTable);
		spark.sql(queryDropTripleTableFixed);

		String createTripleTableRaw = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %1$s(%2$s STRING, %3$s STRING, %4$s STRING) ROW FORMAT SERDE "
						+ "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
						+ "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.+)\\\\s*\\\\.\\\\s*$\")"
						+ "LOCATION '%5$s'",
				name_tripletable + "_raw", column_name_subject, column_name_predicate, column_name_object,
				hdfs_input_directory);
		spark.sql(createTripleTableRaw);

		String createTripleTableFixed = String.format(
				"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING, %4$s STRING) STORED AS PARQUET",
				name_tripletable + "_fixed", column_name_subject, column_name_predicate, column_name_object);
		spark.sql(createTripleTableFixed);

		String repairTripleTableFixed = String.format("INSERT OVERWRITE TABLE %1$s " + "SELECT %2$s, %3$s, trim(%4$s) "
				+ "FROM %5$s " + "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
				+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND "
				+ "NOT(%4$s RLIKE '^\\s*<.*<.*>')  AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\"') ",
				name_tripletable + "_fixed", column_name_subject, column_name_predicate, column_name_object,
				name_tripletable + "_raw");
		spark.sql(repairTripleTableFixed);

		logger.info("Created tripletable with: " + createTripleTableRaw);
		logger.info("Cleaned tripletable created with: " + repairTripleTableFixed);

		String queryRawTriples = String.format("SELECT * FROM %s", name_tripletable + "_raw");
		String queryAllTriples = String.format("SELECT * FROM %s", name_tripletable + "_fixed");

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
	}
}
