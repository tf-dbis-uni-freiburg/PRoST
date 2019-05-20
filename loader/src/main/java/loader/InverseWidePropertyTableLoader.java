package loader;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Loads an IWPT named inverse_wide_property_table.
 * Schema: o|&lt p_0 &gt |... |&lt p_n &gt
 */
public class InverseWidePropertyTableLoader extends PropertyTableLoader {
	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "inverse_properties";
	private static final String IWPT_TABLE_NAME = "inverse_wide_property_table";

	public InverseWidePropertyTableLoader(final Settings settings, final SparkSession spark) {
		super(settings.getDatabaseName(), spark, settings.isIwptPartitionedByObject(), IWPT_TABLE_NAME);
	}

	@Override
	Dataset<Row> loadDataset() {
		final Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(COLUMN_NAME_OBJECT);
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		final Map<String, Boolean> propertiesCardinalitiesMap =
				createPropertiesComplexitiesMap(propertiesCardinalities);
		setPropertiesNames(propertiesCardinalitiesMap.keySet().toArray(new String[0]));
		return createPropertyTableDataset(propertiesCardinalitiesMap, COLUMN_NAME_OBJECT,
				COLUMN_NAME_SUBJECT);
	}
}
