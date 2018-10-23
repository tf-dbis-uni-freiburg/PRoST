package loader;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

/**
 * This abstract class define the parameters and methods for loading an RDF graph into HDFS using Spark SQL.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public abstract class Loader {

	protected SparkSession spark;
	protected String database_name;
	protected String hdfs_input_directory;
	protected static final Logger logger = Logger.getLogger("PRoST");
	public boolean keep_temporary_tables = false;
	public static final String table_format = "parquet";
	public static final String max_length_col_name = "128";
	/**
	 * The separators used in the RDF data.
	 */
	public String line_terminator = "\\n";
	public String column_name_subject = "s";
	public String column_name_predicate = "p";
	public String column_name_object = "o";
	public String name_tripletable = "tripletable";
	protected String[] properties_names;
	public String stats_file_suffix = ".stats";

	public Loader(final String hdfs_input_directory, final String database_name, final SparkSession spark) {
		this.database_name = database_name;
		this.spark = spark;
		this.hdfs_input_directory = hdfs_input_directory;
		// Configurations (they should be working but they are not in Cloudera). Change hive-site.xml.
		// spark.sql("SET hive.exec.dynamic.partition = true");
		// spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
		// spark.sql("SET hive.exec.max.dynamic.partitions = 4000");
		// spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 2000");

		// from now on, set the right database
		useOutputDatabase();
	}

	public abstract void load() throws Exception;

	/**
	 * Replace all not allowed characters of a DB column name by an underscore("_") and return a valid DB column name.
	 * The datastore accepts only characters in the range [a-zA-Z0-9_]
	 *
	 * @param columnName
	 *            column name that will be validated and fixed
	 * @return name of a DB column
	 */
	protected String getValidHiveName(final String columnName) {
		return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	/**
	 * Remove all the tables indicated as parameter.
	 *
	 * @param tableNames
	 *            the names of the tables that will be removed
	 * @return
	 */
	protected void dropTables(final String... tableNames) {
		for (final String tb : tableNames) {
			spark.sql("DROP TABLE " + tb);
		}
		logger.info("Removed tables: " + tableNames);
	}

	protected void useOutputDatabase() {
		spark.sql("CREATE DATABASE IF NOT EXISTS " + database_name);
		spark.sql("USE " + database_name);
		logger.info("Using the database: " + database_name);
	}
}
