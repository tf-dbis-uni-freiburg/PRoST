package loader;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InverseWidePropertyTable extends PropertyTableLoader {
	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "inverse_properties";
	private static final String IWPT_TABLE_NAME = "inverse_wide_property_table";

	public InverseWidePropertyTable(final String databaseName,
									final SparkSession spark, final boolean isPartitioned) {
		super(databaseName, spark, isPartitioned, IWPT_TABLE_NAME);
	}

	@Override
	Dataset<Row> loadDataset() {
		final Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(column_name_object);
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		final Map<String, Boolean> propertiesCardinalitiesMap =
				createPropertiesComplexitiesMap(propertiesCardinalities);
		properties_names = propertiesCardinalitiesMap.keySet().toArray(new String[0]);
		return createPropertyTableDataset(propertiesCardinalitiesMap, column_name_object,
				column_name_subject);
	}
}
