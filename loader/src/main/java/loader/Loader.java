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
	protected final String databaseName;
	protected final String hdfsInputDirectory;
	protected static final Logger LOGGER = Logger.getLogger(Main.class);
	public boolean keepTemporaryFiles = false;
	public static final String TABLE_FORMAT = "parquet";
	/** The separators used in the RDF data. */
	public final String lineTerminator = "\\n";
	public final String subjectColumnName = "s";
	public final String predicateColumnName = "p";
	public final String objectColumnName = "o";
	public final String tripleTableName  = "tripletable";
	protected String[] propertiesNames;
	public final String statsFileSufix = ".stats";
	
	public Loader(String hdfs_input_directory, String database_name, SparkSession spark){
		this.databaseName = database_name;
		this.spark = spark;
		this.hdfsInputDirectory = hdfs_input_directory;
		
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
	protected String getValidHiveName(String columnName) {
		return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
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
		LOGGER.info("Removed tables: " + tableNames);
	}
	
	protected void useOutputDatabase() {
		spark.sql("CREATE DATABASE IF NOT EXISTS " + databaseName);
		spark.sql("USE "  + databaseName);
		LOGGER.info("Using the database: " + databaseName);
	}
}
