package loader;

import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import stats.PropertyStatistics;

/**
 * Builds wide property tables obtained from the join of a WPT with a IWPT on a common resource.
 */
public class JoinedWidePropertyTableLoader extends PropertyTableLoader {
	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String IWPT_PREFIX = "s_";
	private static final String WPT_PREFIX = "o_";
	private static final String JWPT_TABLE_NAME = "joined_wide_property_table";
	private final String joinType;
	private final Settings settings;

	public JoinedWidePropertyTableLoader(final Settings settings, final SparkSession spark, final JoinType joinType) {
		super(settings.getDatabaseName(), spark, settings.isJwptPartitionedByResource(),
				JWPT_TABLE_NAME + "_" + joinType.toString());
		this.joinType = joinType.toString();
		this.settings = settings;
	}

	Dataset<Row> loadDataset() {
		Dataset<Row> iwptDataset;
		Dataset<Row> wptDataset;
		if (settings.isGeneratingWPT()) {
			wptDataset = spark.sql("SELECT * FROM wide_property_table");
		} else {
			final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(settings, spark);
			wptDataset = wptLoader.loadDataset();
		}

		if (settings.isGeneratingIWPT()) {
			iwptDataset = spark.sql("SELECT * FROM inverse_wide_property_table");
		} else {
			final InverseWidePropertyTableLoader iwptLoader = new InverseWidePropertyTableLoader(settings, spark);
			iwptDataset = iwptLoader.loadDataset();
		}

		assert statistics.getProperties().size() > 0 : "No properties information found in statistics. Cannot create "
				+ "JWPT";
		for (final PropertyStatistics stats : statistics.getProperties().values()) {
			String property = stats.getInternalName();
			iwptDataset = iwptDataset.withColumnRenamed(property, IWPT_PREFIX.concat(property));
			wptDataset = wptDataset.withColumnRenamed(property, WPT_PREFIX.concat(property));
		}

		wptDataset = wptDataset.withColumnRenamed(COLUMN_NAME_SUBJECT, COLUMN_NAME_COMMON_RESOURCE);
		iwptDataset = iwptDataset.withColumnRenamed(COLUMN_NAME_OBJECT, COLUMN_NAME_COMMON_RESOURCE);
		return wptDataset.join(iwptDataset,
				scala.collection.JavaConverters.asScalaIteratorConverter(
						Collections.singletonList(COLUMN_NAME_COMMON_RESOURCE).iterator()).asScala().toSeq(), joinType);
	}

	/**
	 * The possible types of join operations.
	 * An outer join will contain all data, and therefore can be used independently from the WPT and IWPT with any
	 * query type.
	 * An inner joined table can only be used with queries of the type ?r0->?r1->?r2, but not ?r0<-?r1->?r2 or
	 * ?r0->?r1<-?r2.
	 * A left inner joined table can be used with queries of the type r0->r1->r2 or ?r0<-?r1->?r2, but not
	 * ?r0->?r1<-?r2.
	 */
	public enum JoinType {
		outer,
		inner,
		leftouter,
	}
}
