package loader;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import statistics.DatabaseStatistics;

/**
 * This abstract class define the parameters and methods for loading an RDF graph into HDFS using Spark SQL.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public abstract class Loader {
	static final Logger logger = Logger.getLogger("PRoST");

	static final String TABLE_FORMAT = "parquet";
	static final String MAX_LENGTH_COL_NAME = "128";
	static final String COLUMN_NAME_SUBJECT = "s";
	static final String COLUMN_NAME_PREDICATE = "p";
	static final String COLUMN_NAME_OBJECT = "o";
	static final String TRIPLETABLE_NAME = "tripletable";

	protected final SparkSession spark;
	private final String databaseName;
	private final DatabaseStatistics statistics;
	private String[] propertiesNames;

	Loader(final String databaseName, final SparkSession spark, final DatabaseStatistics statistics) {
		this.databaseName = databaseName;
		this.spark = spark;
		this.statistics = statistics;
		useOutputDatabase();
	}

	public abstract void load() throws Exception;

	/**
	 * Replace all not allowed characters of a DB column name by an underscore("_") and return a valid DB column name.
	 * The datastore accepts only characters in the range [a-zA-Z0-9_]
	 *
	 * @param columnName column name that will be validated and fixed
	 * @return name of a DB column
	 */
	String getValidHiveName(final String columnName) {
		return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
	}

	private void useOutputDatabase() {
		spark.sql("CREATE DATABASE IF NOT EXISTS " + databaseName);
		spark.sql("USE " + databaseName);
		//logger.info("Using the database: " + databaseName);
	}

	String[] getPropertiesNames() {
		return propertiesNames;
	}

	void setPropertiesNames(final String[] propertiesNames) {
		this.propertiesNames = propertiesNames;
	}

	DatabaseStatistics getStatistics() {
		return statistics;
	}
}
