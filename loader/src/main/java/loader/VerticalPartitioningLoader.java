package loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import loader.ProtobufStats.Graph;
import loader.ProtobufStats.TableStats;

/**
 * Build the VP, i.e. a table for each predicate.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 *
 */
public class VerticalPartitioningLoader extends Loader {
	private final boolean computeStatistics;

	public VerticalPartitioningLoader(final String hdfs_input_directory, final String database_name,
			final SparkSession spark, final boolean computeStatistics) {
		super(hdfs_input_directory, database_name, spark);
		this.computeStatistics = computeStatistics;
	}

	@Override
	public void load() {
		logger.info("PHASE 3: creating the VP tables...");

		if (properties_names == null) {
			properties_names = extractProperties();
		}

		final Vector<TableStats> tables_stats = new Vector<>();

		for (int i = 0; i < properties_names.length; i++) {
			final String property = properties_names[i];
			final String createVPTableFixed =
					String.format("CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING) STORED AS PARQUET",
							"vp_" + getValidHiveName(property), column_name_subject, column_name_object);
			// Commented code is partitioning by subject
			/*
			 * String createVPTableFixed = String.format(
			 * "CREATE TABLE  IF NOT EXISTS  %1$s(%3$s STRING) PARTITIONED BY (%2$s STRING) STORED AS PARQUET"
			 * , "vp_" + this.getValidHiveName(property), column_name_subject, column_name_object);
			 */
			spark.sql(createVPTableFixed);

			final String populateVPTable = String.format(
					"INSERT OVERWRITE TABLE %1$s " + "SELECT %2$s, %3$s " + "FROM %4$s WHERE %5$s = '%6$s' ",
					"vp_" + getValidHiveName(property), column_name_subject, column_name_object, name_tripletable,
					column_name_predicate, property);
			// Commented code is partitioning by subject
			/*
			 * String populateVPTable = String.format( "INSERT OVERWRITE TABLE %1$s PARTITION (%2$s) "
			 * + "SELECT %3$s, %2$s " + "FROM %4$s WHERE %5$s = '%6$s' ", "vp_" +
			 * this.getValidHiveName(property), column_name_subject, column_name_object,
			 * name_tripletable, column_name_predicate, property);
			 */
			spark.sql(populateVPTable);

			// calculate stats
			final Dataset<Row> table_VP = spark.sql("SELECT * FROM " + "vp_" + getValidHiveName(property));

			if (computeStatistics) {
				tables_stats.add(calculate_stats_table(table_VP, getValidHiveName(property)));
			}

			logger.info("Created VP table for the property: " + property);
			final List<Row> sampledRowsList = table_VP.limit(3).collectAsList();
			logger.info("First 3 rows sampled (or less if there are less): " + sampledRowsList);
		}

		// save the stats in a file with the same name as the output database
		if (computeStatistics) {
			save_stats(database_name, tables_stats);
		}

		logger.info("Vertical Partitioning completed. Loaded " + String.valueOf(properties_names.length) + " tables.");

	}

	/*
	 * calculate the statistics for a single table: size, number of distinct subjects and
	 * isComplex. It returns a protobuf object defined in ProtobufStats.proto
	 */
	private TableStats calculate_stats_table(final Dataset<Row> table, final String tableName) {
		final TableStats.Builder table_stats_builder = TableStats.newBuilder();

		// calculate the stats
		final int table_size = (int) table.count();
		final int distinct_subjects = (int) table.select(column_name_subject).distinct().count();
		final boolean is_complex = table_size != distinct_subjects;

		table_stats_builder.setSize(table_size).setDistinctSubjects(distinct_subjects).setIsComplex(is_complex)
				.setName(tableName);

		if (spark.catalog().tableExists("inverse_properties")) {
			final String query = new String("select is_complex from inverse_properties where p='" + tableName + "'");
			final boolean isInverseComplex = spark.sql(query.toString()).head().getInt(0) == 1;
			// put them in the protobuf object
			table_stats_builder.setIsInverseComplex(isInverseComplex);
		}

		logger.info(
				"Adding these properties to Protobuf object. Table size:" + table_size + ", " + "Distinct subjects: "
						+ distinct_subjects + ", Is complex:" + is_complex + ", " + "tableName:" + tableName);

		return table_stats_builder.build();
	}

	/*
	 * save the statistics in a serialized file
	 */
	private void save_stats(final String name, final List<TableStats> table_stats) {
		final Graph.Builder graph_stats_builder = Graph.newBuilder();

		graph_stats_builder.addAllTables(table_stats);
		graph_stats_builder.setArePrefixesActive(arePrefixesUsed());
		final Graph serialized_stats = graph_stats_builder.build();

		FileOutputStream f_stream; // s
		File file;
		try {
			file = new File(name + stats_file_suffix);
			f_stream = new FileOutputStream(file);
			serialized_stats.writeTo(f_stream);
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}
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

	/**
	 * Checks if there is at least one property that uses prefixes.
	 */
	private boolean arePrefixesUsed() {
		for (final String property : properties_names) {
			if (property.contains(":")) {
				return true;
			}
		}
		return false;
	}

}
