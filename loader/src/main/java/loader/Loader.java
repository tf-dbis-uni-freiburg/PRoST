package loader;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import run.Main;

/**
 * This abstract class define the parameters and methods for loading an RDF graph into HDFS using Spark SQL.
 * 
 * @author Matteo Cossu
 *
 */
public abstract class Loader {
	
	public abstract void load();
	
	protected SparkSession spark;
	protected String database_name;
	protected String hdfs_input_directory;
	protected static final Logger logger = Logger.getLogger(Main.class);
	public boolean keep_temporary_tables = false;
	public static final String table_format = "parquet";
	/** The separators used in the RDF data. */
	public String field_terminator = "\\t";
	public String line_terminator = "\\n";
	public String column_name_subject = "s";
	public String column_name_predicate = "p";
	public String column_name_object = "o";
	public String name_tripletable  = "tripletable";
	
	public Loader(String hdfs_input_directory, String database_name, SparkSession spark){
		this.database_name = database_name;
		this.spark = spark;
		this.hdfs_input_directory = hdfs_input_directory;
		
		// from now on, set the right database
		this.useOutputDatabase();
	}
	
	/**
	 * Replace all not allowed characters of a DB column name by an underscore("_") and 
	 * return a valid DB column name.
	 * The datastore  accepts only characters in the range [a-zA-Z0-9_]
	 * 
	 * @param columnName
	 *            column name that will be validated and fixed
	 * @return name of a DB column
	 */
	protected String getValidColumnName(String columnName) {
		return columnName.replaceAll("[^a-zA-Z0-9_]", "_");
	}
	
	
	/**
	 * Remove all the tables indicated as parameter.
	 * 
	 * @param tableNames
	 *            the names of the tables that will be removed
	 * @return 
	 */
	protected void dropTables(String... tableNames) {
		for (String tb : tableNames)
			spark.sql("DROP TABLE " + tb);
		logger.info("Removed tables: " + tableNames);
	}
	
	protected void useOutputDatabase() {
		spark.sql("CREATE DATABASE IF NOT EXISTS " + database_name);
		spark.sql("USE "  + database_name);
		logger.info("Using the database: " + database_name);
	}

}
