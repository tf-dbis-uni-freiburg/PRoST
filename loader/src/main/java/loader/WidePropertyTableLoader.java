package loader;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WidePropertyTableLoader extends PropertyTableLoader {

	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "properties";
	private static final String WPT_TABLE_NAME = "wide_property_table";

	public WidePropertyTableLoader(final Settings settings,
								   final SparkSession spark) {
		super(settings.getDatabaseName(), spark, settings.isWptPartitionedBySubject(), WPT_TABLE_NAME);
	}

	@Override
	Dataset<Row> loadDataset() {
		final Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(COLUMN_NAME_SUBJECT);
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		final Map<String, Boolean> propertiesCardinalitiesMap =
				createPropertiesComplexitiesMap(propertiesCardinalities);
		setPropertiesNames(propertiesCardinalitiesMap.keySet().toArray(new String[0]));
		return createPropertyTableDataset(propertiesCardinalitiesMap, COLUMN_NAME_SUBJECT,
				COLUMN_NAME_OBJECT);
	}
}
