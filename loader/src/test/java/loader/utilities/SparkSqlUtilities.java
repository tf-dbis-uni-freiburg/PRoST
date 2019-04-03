package loader.utilities;

import org.apache.spark.sql.SparkSession;

/**
 * Utilities related to Spark SQL (only for test scope)
 *
 * @author Victor Anthony Arrascue Ayala
 */
public class SparkSqlUtilities {
	/**
	 * Sets the spark session to be able to partition physically.
	 */
	public static void enableSessionForPhysicalPartitioning(final SparkSession spark) {
		spark.sql("SET hive.exec.dynamic.partition = true");
		spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
		//spark.sql("SET hive.exec.max.dynamic.partitions = 4000");
		//spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 2000");
	}
}
