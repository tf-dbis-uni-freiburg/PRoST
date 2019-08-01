package loader;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import statistics.DatabaseStatistics;

public class EmergentSchemaLoader {
	private static final String TABLE_PREFIX = "ES_";
	private static final String WPT_NAME = "wide_property_table";
	static final String TABLE_FORMAT = "parquet";

	public static void load(final DatabaseStatistics statistics, final SQLContext sqlContext) {
		//Compute supersets

		//for each superset: saveTable, saveStats
	}

	private void saveTable(final HashSet<String> properties, final DatabaseStatistics statistics,
						   final SQLContext sqlContext, final String tableName) {
		final ArrayList<String> selectElements = new ArrayList<>();
		final ArrayList<String> whereElements = new ArrayList<>();

		for (final String property : properties) {
			final String internalName = statistics.getProperties().get(property).getInternalName();
			selectElements.add(internalName);
			whereElements.add(internalName + "IS NOT NULL");
		}

		String query = "SELECT " + String.join(", ", selectElements);
		query += " FROM " + WPT_NAME;
		query += " WHERE " + String.join(" OR ", whereElements);

		final Dataset<Row> emergentTable = sqlContext.sql(query);
		emergentTable.write().mode(SaveMode.Overwrite).format(TABLE_FORMAT).partitionBy("r")
				.saveAsTable(tableName);

		statistics.addEmergentSchema(properties,tableName);
	}
}
