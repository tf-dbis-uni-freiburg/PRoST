package loader;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class VerticalPartitioningLoader extends Loader {

	public VerticalPartitioningLoader(String hdfs_input_directory,
			String database_name, SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}

	@Override
	public void load() {

		logger.info("Beginning the creation of VP tables.");
		
		if (this.properties_names.equals(null)){
			logger.error("Properties not calculated yet. Extracting them");
			this.properties_names = extractProperties();
		}
		
		// TODO: these jobs should be submitted in parallel threads
		// leaving the control to the YARN scheduler
		for(String property : this.properties_names){
			Dataset<Row> table_VP = spark.sql("SELECT s AS s, o AS o FROM tripletable WHERE p='" + property + "'");
			String table_name_VP = "vp_" + this.getValidHiveName(property);
			table_VP.write().mode(SaveMode.Overwrite).saveAsTable(table_name_VP);
			logger.info("Created VP table for the property: " + property);
		}
		
		logger.info("Vertical Partitioning completed. Loaded " + String.valueOf(this.properties_names.length) + " tables.");
		
	}
	
	private String[] extractProperties() {
		List<Row> props = spark.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s",
				column_name_predicate, name_tripletable)).collectAsList();
		String[] result_properties = new String[props.size()];
		
		for (int i = 0; i < props.size(); i++) {
			result_properties[i] = props.get(i).getString(0);
		}
		return result_properties;
	}

}
