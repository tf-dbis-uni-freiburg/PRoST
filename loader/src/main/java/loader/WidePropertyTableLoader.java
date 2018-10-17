package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class WidePropertyTableLoader extends PropertyTableLoader {
	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "properties";
	private static final String WPT_TABLE_NAME = "wide_property_table";

	public WidePropertyTableLoader(final String hdfsInputDirectory, final String databaseName,
								   final SparkSession spark, boolean isPartitioned){
		super(hdfsInputDirectory, databaseName, spark, isPartitioned, WPT_TABLE_NAME);
	}

	@Override
	Dataset<Row> loadDataset() {
		Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(column_name_subject);
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		Map<String, Boolean> propertiesCardinalitiesMap = createPropertiesComplexitiesMap(propertiesCardinalities);
		properties_names = propertiesCardinalitiesMap.keySet().toArray(new String[0]);
		return createPropertyTableDataset(propertiesCardinalitiesMap, column_name_subject,
				column_name_object);
	}
}
