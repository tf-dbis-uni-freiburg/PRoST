package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class InverseWidePropertyTable extends PropertyTableLoader {
	private static final String PROPERTIES_CARDINALITIES_TABLE_NAME = "inverse_properties";
	private static final String IWPT_TABLE_NAME = "inverse_wide_property_table";

	public InverseWidePropertyTable(final String hdfsInputDirectory, final String databaseName,
							 final SparkSession spark, boolean isPartitioned){
		super(hdfsInputDirectory, databaseName, spark, isPartitioned, IWPT_TABLE_NAME);
	}

	@Override
	Dataset<Row> loadDataset(){
		Dataset<Row> propertiesCardinalities = calculatePropertiesComplexity(column_name_object);
		saveTable(propertiesCardinalities, PROPERTIES_CARDINALITIES_TABLE_NAME);
		Map<String,Boolean> propertiesCardinalitiesMap = createPropertiesComplexitiesMap(propertiesCardinalities);
		properties_names = propertiesCardinalitiesMap.keySet().toArray(new String[0]);
		Dataset<Row> iwpt =  createPropertyTableDataset(propertiesCardinalitiesMap, column_name_object,column_name_subject);

		// renames the column so that its name is consistent with the non
		// inverse Wide Property Table.This guarantees that any method that access a Property
		// Table can be used with a Inverse Property Table without any changes
		iwpt = iwpt.withColumnRenamed(column_name_object, column_name_subject);

		return iwpt;
	}
}
