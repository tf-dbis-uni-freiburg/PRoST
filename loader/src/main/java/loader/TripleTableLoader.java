package loader;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


/**
 * Class that constructs the triple table. It is created as external table, so it can use the input file
 * directly without loosing time to replicate the data, since the triple table will be used only for other
 * models e.g. Property Table, Vertical Partitioning.
 * 
 * @author Matteo Cossu
 *
 */
public class TripleTableLoader extends Loader {

	public TripleTableLoader(String hdfs_input_directory, String database_name,
			SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}
	
	@Override
	public void load() {
		
		String createTripleTable = String.format(
				"CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s STRING, %s STRING, %s STRING) ROW FORMAT  SERDE"
						+ "'org.apache.hadoop.hive.serde2.RegexSerDe'  WITH SERDEPROPERTIES "
						+ "( \"input.regex\" = \"(\\\\S+)\\\\s+(\\\\S+)\\\\s+(.*)\")"
						+ "LOCATION '%s'",
						tripleTableName, subjectColumnName, predicateColumnName, objectColumnName, hdfsInputDirectory);

		spark.sql(createTripleTable);
		LOGGER.info("Created tripletable");
	}
	
	public void load_ntriples() {
		String ds = hdfsInputDirectory;
		Dataset<Row> triple_table_file = spark.read().text(ds);

		
		String triple_regex = build_triple_regex();

		Dataset<Row> triple_table = triple_table_file.select(
				functions.regexp_extract(functions.col("value"), triple_regex, 1).alias(this.subjectColumnName),
				functions.regexp_extract(functions.col("value"), triple_regex, 2).alias(this.predicateColumnName),
				functions.regexp_extract(functions.col("value"), triple_regex, 3).alias(this.objectColumnName));
		
		triple_table.createOrReplaceTempView(tripleTableName);
		LOGGER.info("Created tripletable");
	}
	
	// this method exists for the sake of clarity instead of a constant String
	// Therefore, it should be called only once
	private static String build_triple_regex() {
		String uri_s = "<(?:[^:]+:[^\\s\"<>]+)>";
		String literal_s = "\"(?:[^\"\\\\]*(?:\\.[^\"\\\\]*)*)\"(?:@([a-z]+(?:-[a-zA-Z0-9]+)*)|\\^\\^" + uri_s + ")?";
		String subject_s = "(" + uri_s + "|" + literal_s + ")";
		String predicate_s = "(" + uri_s + ")";
		String object_s = "(" + uri_s + "|" + literal_s + ")";
		String space_s = "[ \t]+";
		return "[ \\t]*" + subject_s + space_s + predicate_s + space_s + object_s + "[ \\t]*\\.*[ \\t]*(#.*)?";
	}

}
