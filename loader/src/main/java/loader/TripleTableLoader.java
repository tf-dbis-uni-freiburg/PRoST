package loader;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



/**
 * Class that constructs a triples table. First, the loader creates an external table ("raw").
 * The data is read using SerDe capabilities and by means of a regular expresion.
 * An additional table ("fixed") is created to make sure that  only valid triples are passed to the 
 * next stages in which other models e.g. Property Table, or Vertical Partitioning are built.
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
		String queryDropTripleTableFixed = String.format("DROP TABLE IF EXISTS %s", name_tripletable+"_fixed");

		spark.sql(queryDropTripleTable);
		spark.sql(queryDropTripleTableFixed);
		
		String createTripleTableRaw = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s STRING, %s STRING, %s STRING) ROW FORMAT  SERDE"
						+ "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
						+ "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.+)\\\\s*\\\\.\\\\s*$\")" + "LOCATION '%s'",
				name_tripletable+"_raw", column_name_subject, column_name_predicate, column_name_object, hdfs_input_directory);
		spark.sql(createTripleTableRaw);
		
		String createTripleTableFixed = String.format(
				"CREATE TABLE  IF NOT EXISTS  %s(%s STRING, %s STRING, %s STRING)", 
					name_tripletable+"_fixed", column_name_subject, column_name_predicate, column_name_object);
		spark.sql(createTripleTableFixed);
		
		String repairTripleTableFixed = String.format(
				"INSERT OVERWRITE TABLE %1$s SELECT %2$s, %3$s, trim(%4$s) "
						+ "FROM %5$s "
						+ "WHERE %2$s is not null AND %3$s is not null AND %4$s is not null AND "
						+ "NOT(%2$s RLIKE '^\\s*\\.\\s*$')  AND NOT(%3$s RLIKE '^\\s*\\.\\s*$') AND NOT(%4$s RLIKE '^\\s*\\.\\s*$') AND "
						+ "NOT(%4$s RLIKE '^\\s*<.*<.*>')  AND NOT(%4$s RLIKE '(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\".*(?<!\\u005C\\u005C)\"') ",
				name_tripletable+"_fixed", column_name_subject, column_name_predicate, column_name_object, name_tripletable+"_raw");
		spark.sql(repairTripleTableFixed);
		
		logger.info("Created tripletable with: " + createTripleTableRaw);
		logger.info("Cleaned tripletable created with: " + repairTripleTableFixed);
				
		String queryAllTriples = String.format("SELECT * FROM %s", name_tripletable+"_fixed");
		Dataset<Row>  allTriples = spark.sql(queryAllTriples);
		if (allTriples.count()==0) {
			logger.error("Either your HDFS path does not contain any files or no triples were accepted in the given format (nt)");
			logger.error("The program will stop here.");
			throw new Exception("Empty HDFS directory or empty files within.");
		}
		List cleanedList = allTriples.limit(10).collectAsList();
		logger.info("First 10 cleaned triples (less if there are less): " + cleanedList);
	}

	// this method exists for the sake of clarity instead of a constant String
	// Therefore, it should be called only once
	/*
	private static String build_triple_regex() {
		String uri_s = "<(?:[^:]+:[^\\s\"<>]+)>";
		String literal_s = "\"(?:[^\"\\\\]*(?:\\.[^\"\\\\]*)*)\"(?:@([a-z]+(?:-[a-zA-Z0-9]+)*)|\\^\\^" + uri_s + ")?";
		String subject_s = "(" + uri_s + "|" + literal_s + ")";
		String predicate_s = "(" + uri_s + ")";
		String object_s = "(" + uri_s + "|" + literal_s + ")";
		String space_s = "[ \t]+";
		return "[ \\t]*" + subject_s + space_s + predicate_s + space_s + object_s + "[ \\t]*\\.*[ \\t]*(#.*)?";
	}*/
}
