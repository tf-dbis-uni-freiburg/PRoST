package loader;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WidePropertyTableLoader extends PropertyTableLoader {

	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "properties";
	private static final String WPT_TABLE_NAME = "wide_property_table";

	public WidePropertyTableLoader(final String databaseName,
								   final SparkSession spark, final boolean isPartitioned) {
		super(databaseName, spark, isPartitioned, WPT_TABLE_NAME);
	}

	@Override
	Dataset<Row> loadDataset() {
		final Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(column_name_subject);
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		final Map<String, Boolean> propertiesCardinalitiesMap =
				createPropertiesComplexitiesMap(propertiesCardinalities);
		properties_names = propertiesCardinalitiesMap.keySet().toArray(new String[0]);
		return createPropertyTableDataset(propertiesCardinalitiesMap, column_name_subject,
				column_name_object);
	}
}
