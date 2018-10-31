package loader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import stats.StatisticsWriter;

/**
 * Build the VP, i.e. a table for each predicate.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 *
 */
public class VerticalPartitioningLoader extends Loader {

	public VerticalPartitioningLoader(final String hdfs_input_directory, final String database_name,
			final SparkSession spark) {
		super(hdfs_input_directory, database_name, spark);
	}

	@Override
	public void load() {
		logger.info("PHASE 3: creating the VP tables...");

		if (properties_names == null) {
			properties_names = extractProperties();
		}

		for (int i = 0; i < properties_names.length; i++) {
			final String property = properties_names[i];
			final String createVPTableFixed = String.format(
					"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING) STORED AS PARQUET",
					"vp_" + getValidHiveName(property), column_name_subject, column_name_object);
			// Commented code is partitioning by subject
			/*
			 * String createVPTableFixed = String.format(
			 * "CREATE TABLE  IF NOT EXISTS  %1$s(%3$s STRING) PARTITIONED BY (%2$s STRING) STORED AS PARQUET"
			 * , "vp_" + this.getValidHiveName(property), column_name_subject,
			 * column_name_object);
			 */
			spark.sql(createVPTableFixed);

			final String populateVPTable = String.format(
					"INSERT OVERWRITE TABLE %1$s " + "SELECT %2$s, %3$s " + "FROM %4$s WHERE %5$s = '%6$s' ",
					"vp_" + getValidHiveName(property), column_name_subject, column_name_object, name_tripletable,
					column_name_predicate, property);
			// Commented code is partitioning by subject
			/*
			 * String populateVPTable = String.format(
			 * "INSERT OVERWRITE TABLE %1$s PARTITION (%2$s) " + "SELECT %3$s, %2$s " +
			 * "FROM %4$s WHERE %5$s = '%6$s' ", "vp_" + this.getValidHiveName(property),
			 * column_name_subject, column_name_object, name_tripletable,
			 * column_name_predicate, property);
			 */
			spark.sql(populateVPTable);

			// calculate stats
			final Dataset<Row> vpTableDataset = spark.sql("SELECT * FROM " + "vp_" + getValidHiveName(property));
			StatisticsWriter.getInstance().addStatsTable(vpTableDataset, getValidHiveName(property),
					column_name_subject);

			logger.info("Created VP table for the property: " + property);
			final List<Row> sampledRowsList = vpTableDataset.limit(3).collectAsList();
			logger.info("First 3 rows sampled (or less if there are less): " + sampledRowsList);
		}
		logger.info("Vertical Partitioning completed. Loaded " + String.valueOf(properties_names.length) + " tables.");
	}

	private String[] extractProperties() {
		final List<Row> props = spark
				.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s", column_name_predicate, name_tripletable))
				.collectAsList();
		final String[] properties = new String[props.size()];
		for (int i = 0; i < props.size(); i++) {
			properties[i] = props.get(i).getString(0);
		}
		final List<String> propertiesList = Arrays.asList(properties);
		logger.info("Number of distinct predicates found: " + propertiesList.size());
		final String[] cleanedProperties = handleCaseInsPred(properties);
		final List<String> cleanedPropertiesList = Arrays.asList(cleanedProperties);
		logger.info("Final list of predicates: " + cleanedPropertiesList);
		logger.info("Final number of distinct predicates: " + cleanedPropertiesList.size());
		return cleanedProperties;
	}

	private String[] handleCaseInsPred(final String[] properties) {
		final Set<String> seenPredicates = new HashSet<>();
		final Set<String> originalRemovedPredicates = new HashSet<>();
		final Set<String> propertiesSet = new HashSet<>(Arrays.asList(properties));

		final Iterator<String> it = propertiesSet.iterator();
		while (it.hasNext()) {
			final String predicate = it.next();
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
		final String[] cleanedProperties = propertiesSet.toArray(new String[propertiesSet.size()]);
		return cleanedProperties;
	}
}
