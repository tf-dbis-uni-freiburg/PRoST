package loader;

import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import statistics.DatabaseStatistics;
import statistics.PropertyStatistics;

/**
 * Builds wide property tables obtained from the join of a WPT with a IWPT on a common resource.
 * The tables may be joined with either a outer, inner, or left outer join operation, and are named accordingly
 * (joined_wide_property_table_outer, joined_wide_property_table_inner, or joined_wide_property_table_leftouter)
 * Schema: r|o_&lt p_0 &gt |... |o_&lt p_n &gt|s_&lt p_0 &gt |... |s_&lt p_n &gt
 */
public class JoinedWidePropertyTableLoader extends PropertyTableLoader {
	private static final String COLUMN_NAME_COMMON_RESOURCE = "r";
	private static final String IWPT_PREFIX = "s_";
	private static final String WPT_PREFIX = "o_";
	private static final String JWPT_TABLE_NAME = "joined_wide_property_table";
	private final String joinType;
	private final Settings settings;

	public JoinedWidePropertyTableLoader(final Settings settings, final SparkSession spark, final JoinType joinType,
										 final DatabaseStatistics statistics) {
		super(settings.getDatabaseName(), spark, settings.isJwptPartitionedByResource(),
				JWPT_TABLE_NAME + "_" + joinType.toString(), statistics);
		this.joinType = joinType.toString();
		this.settings = settings;
	}

	Dataset<Row> loadDataset() {
		Dataset<Row> iwptDataset;
		Dataset<Row> wptDataset;
		if (settings.isGeneratingWPT()) {
			wptDataset = spark.sql("SELECT * FROM wide_property_table");
		} else {
			final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(settings, spark,
					this.getStatistics());
			wptDataset = wptLoader.loadDataset();
		}

		if (settings.isGeneratingIWPT()) {
			iwptDataset = spark.sql("SELECT * FROM inverse_wide_property_table");
		} else {
			final InverseWidePropertyTableLoader iwptLoader = new InverseWidePropertyTableLoader(settings, spark,
					this.getStatistics());
			iwptDataset = iwptLoader.loadDataset();
		}

		//TODO get internal name from schema instead of stats
		assert this.getStatistics().getProperties().values().size() > 0
				: "No properties information found in statistics. Cannot create JWPT";
		for (final PropertyStatistics stats : this.getStatistics().getProperties().values()) {
			final String property = stats.getInternalName();
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
