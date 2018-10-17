package loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class JoinedWidePropertyTable extends PropertyTableLoader {
	private static final String IWPT_PREFIX = "s_";
	private static final String WPT_PREFIX = "o_";
	private static final String JWPT_TABLE_NAME = "joined_wide_property_table";

	public JoinedWidePropertyTable(final String hdfsInputDirectory, final String databaseName,
							final SparkSession spark, boolean isPartitioned){
		super(hdfsInputDirectory,databaseName,spark,isPartitioned, JWPT_TABLE_NAME);
	}

	Dataset<Row> loadDataset(){
		InverseWidePropertyTable iwptLoader = new InverseWidePropertyTable(hdfs_input_directory,database_name,spark,
				isPartitioned);
		Dataset<Row> iwptDataset = iwptLoader.loadDataset();
		for (final String property : iwptLoader.properties_names) {
			iwptDataset = iwptDataset.withColumnRenamed(property, IWPT_PREFIX.concat(property));
		}

		WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(hdfs_input_directory,database_name,spark, isPartitioned);
		Dataset<Row> wptDataset = wptLoader.loadDataset();
		for (final String property : wptLoader.properties_names) {
			wptDataset = wptDataset.withColumnRenamed(property, WPT_PREFIX.concat(property));
		}

		return wptDataset.join(iwptDataset, scala.collection.JavaConverters
				.asScalaIteratorConverter(Arrays.asList("s").iterator()).asScala().toSeq(), "outer");
	}
}
