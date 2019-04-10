package loader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import stats.DatabaseStatistics;
import stats.PropertyStatistics;

/**
 * Build the VP, i.e. a table for each predicate.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class VerticalPartitioningLoader extends Loader {
	private final boolean isPartitioning;

	public VerticalPartitioningLoader(final String databaseName, final SparkSession spark,
									  final boolean isPartitioning) {
		super(databaseName, spark);
		this.isPartitioning = isPartitioning;
	}

	public VerticalPartitioningLoader(final String databaseName, final SparkSession spark,
									  final boolean isPartitioning, final DatabaseStatistics statistics) {
		super(databaseName, spark, statistics);
		this.isPartitioning = isPartitioning;
	}

	@Override
	public void load() {
		logger.info("PHASE 3: creating the VP tables...");

		if (getPropertiesNames() == null) {
			setPropertiesNames(extractProperties());
		}

		for (final String property : getPropertiesNames()) {
			final String createVPTableFixed;
			if (!isPartitioning) {
				createVPTableFixed = String.format("CREATE TABLE IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING) STORED "
								+ "AS PARQUET",
						"vp_" + getValidHiveName(property), COLUMN_NAME_SUBJECT, COLUMN_NAME_OBJECT);
			} else {
				createVPTableFixed = String.format(
						"CREATE TABLE  IF NOT EXISTS  %1$s(%3$s STRING) PARTITIONED BY (%2$s STRING) STORED AS PARQUET",
						"vp_" + this.getValidHiveName(property), COLUMN_NAME_SUBJECT, COLUMN_NAME_OBJECT);
			}
			spark.sql(createVPTableFixed);

			final String populateVPTable;
			if (!isPartitioning) {
				populateVPTable = String.format(
						"INSERT OVERWRITE TABLE %1$s " + "SELECT %2$s, %3$s " + "FROM %4$s WHERE %5$s = '%6$s' ",
						"vp_" + getValidHiveName(property), COLUMN_NAME_SUBJECT, COLUMN_NAME_OBJECT, TRIPLETABLE_NAME,
						COLUMN_NAME_PREDICATE, property);
			} else {
				populateVPTable = String.format(
						"INSERT OVERWRITE TABLE %1$s PARTITION (%2$s) " + "SELECT %3$s, %2$s "
								+ "FROM %4$s WHERE %5$s = '%6$s' ", "vp_" + this.getValidHiveName(property),
						COLUMN_NAME_SUBJECT, COLUMN_NAME_OBJECT, TRIPLETABLE_NAME,
						COLUMN_NAME_PREDICATE, property);
			}
			spark.sql(populateVPTable);

			// calculate stats
			final Dataset<Row> vpTableDataset = spark.sql("SELECT * FROM " + "vp_" + getValidHiveName(property));

			if (statistics != null) {
				statistics.getProperties().put(property, new PropertyStatistics(vpTableDataset,
						getValidHiveName(property)));
			}
			logger.info("Created VP table for the property: " + property);
			final List<Row> sampledRowsList = vpTableDataset.limit(3).collectAsList();
			logger.info("First 3 rows sampled (or less if there are less): " + sampledRowsList);
		}
		logger.info("Vertical Partitioning completed. Loaded " + getPropertiesNames().length + " tables.");
	}


	private String[] extractProperties() {
		final List<Row> props = spark
				.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s", COLUMN_NAME_PREDICATE, TRIPLETABLE_NAME))
				.collectAsList();
		final String[] properties = new String[props.size()];
		for (int i = 0; i < props.size(); i++) {
			properties[i] = props.get(i).getString(0);
		}
		final List<String> propertiesList = Arrays.asList(properties);
		logger.info("Number of distinct predicates found: " + propertiesList.size());
		final String[] cleanedProperties = handleCaseInsensitivePredicates(properties);
		final List<String> cleanedPropertiesList = Arrays.asList(cleanedProperties);
		logger.info("Final list of predicates: " + cleanedPropertiesList);
		logger.info("Final number of distinct predicates: " + cleanedPropertiesList.size());
		return cleanedProperties;
	}

	private String[] handleCaseInsensitivePredicates(final String[] properties) {
		final Set<String> seenPredicates = new HashSet<>();
		final Set<String> originalRemovedPredicates = new HashSet<>();
		final Set<String> propertiesSet = new HashSet<>(Arrays.asList(properties));

		for (final String predicate : propertiesSet) {
			if (seenPredicates.contains(predicate.toLowerCase())) {
				originalRemovedPredicates.add(predicate);
			} else {
				seenPredicates.add(predicate.toLowerCase());
			}
		}
		for (final String predicateToBeRemoved : originalRemovedPredicates) {
			propertiesSet.remove(predicateToBeRemoved);
		}
		if (originalRemovedPredicates.size() > 0) {
			logger.info("The following predicates had to be removed from the list of predicates "
					+ "(it is case-insensitive equal to another predicate): " + originalRemovedPredicates);
		}
		return propertiesSet.toArray(new String[0]);
	}
}
