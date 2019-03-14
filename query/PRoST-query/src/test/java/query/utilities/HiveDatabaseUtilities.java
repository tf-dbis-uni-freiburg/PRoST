package query.utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * The purpose of this class is to easily create tables which are registered in
 * a Hive Database. In this way queries can be run on an similar way than in a
 * cluster mode.
 * 
 * @author Victor Anthony Arrascue Ayala
 *
 */
public class HiveDatabaseUtilities {
	public static final String tt = "tripletable";
	public static final String wpt = "wide_property_name";
	public static final String iwpt = "inverse_wide_property_name";
	public static final String jwpt = "join_wide_property_name";
	public static final String vp = "";
	public static final int numSupportedStrategies = 5;
	// Parquet not used (it is better for testing purposes
	public static final String tableFormat = "text";

	/**
	 * This method creates a new database using the name passed as the first
	 * argument. If the database exists it is dropped and a new one is created.
	 * Then the triples passed as the second arguments are used to generate
	 * tables in the database according to all possible partitioning strategies
	 * defined in the loader.
	 * 
	 * Only a default physical partitioning strategy is considered since the
	 * goal is test the correctness of the outcome. The loader verifies that
	 * triples are consistently stored according to different physical
	 * partitioning strategies.
	 * 
	 * @param databaseName
	 * @param tableName
	 * @param triples
	 * @param spark
	 */
	public static void writeTriplesToDatabase(String databaseName, Dataset<TripleBean> triples, SparkSession spark) {
		// First we drop the database
		spark.sql("DROP  DATABASE IF EXISTS " + databaseName + " CASCADE");
		spark.sql("CREATE DATABASE IF NOT EXISTS " + databaseName);
		spark.sql("USE " + databaseName);

		triples.write().saveAsTable(HiveDatabaseUtilities.tt);		
		//We create the stats file:		
	}	
}
