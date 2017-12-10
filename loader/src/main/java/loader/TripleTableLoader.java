package loader;

import org.apache.spark.sql.SparkSession;


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
				"CREATE EXTERNAL TABLE IF NOT EXISTS %s(%s STRING, %s STRING, %s STRING) ROW FORMAT DELIMITED"
						+ " FIELDS TERMINATED BY '%s'  LINES TERMINATED BY '%s' LOCATION '%s'",
						name_tripletable  , column_name_subject, column_name_predicate, column_name_object,
				field_terminator, line_terminator, hdfs_input_directory);

		spark.sql(createTripleTable);
		
		
		//spark.sql("show databases").show();
		//spark.sql("show tables from prost_todelete." + name_tripletable).show();
		
		spark.sql("show databases").show();
		spark.sql("show tables from prost_todelete").show();
		spark.sql("describe "+name_tripletable).show();
		logger.info("Created tripletable");
	}
}
