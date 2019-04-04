package loader;

import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinedWidePropertyTable extends PropertyTableLoader {
	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String IWPT_PREFIX = "s_";
	private static final String WPT_PREFIX = "o_";
	private static final String JWPT_TABLE_NAME = "joined_wide_property_table";

	public JoinedWidePropertyTable(final String databaseName,
								   final SparkSession spark, final boolean isPartitioned) {
		super(databaseName, spark, isPartitioned, JWPT_TABLE_NAME);
	}

	Dataset<Row> loadDataset() {
		final InverseWidePropertyTable iwptLoader = new InverseWidePropertyTable(getDatabaseName(), spark,
				isPartitioned);
		Dataset<Row> iwptDataset = iwptLoader.loadDataset();
		for (final String property : iwptLoader.getPropertiesNames()) {
			iwptDataset = iwptDataset.withColumnRenamed(property, IWPT_PREFIX.concat(property));
		}

		final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(getDatabaseName(), spark, isPartitioned);
		Dataset<Row> wptDataset = wptLoader.loadDataset();
		for (final String property : wptLoader.getPropertiesNames()) {
			wptDataset = wptDataset.withColumnRenamed(property, WPT_PREFIX.concat(property));
		}

		wptDataset = wptDataset.withColumnRenamed(COLUMN_NAME_SUBJECT, COLUMN_NAME_COMMON_RESOURCE);
		iwptDataset = iwptDataset.withColumnRenamed(COLUMN_NAME_OBJECT, COLUMN_NAME_COMMON_RESOURCE);
		return wptDataset.join(iwptDataset,
				scala.collection.JavaConverters.asScalaIteratorConverter(
						Collections.singletonList(COLUMN_NAME_COMMON_RESOURCE).iterator()).asScala().toSeq(), "outer");
	}
}
