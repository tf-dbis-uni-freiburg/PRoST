package loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple2;

/**
 * Class that constructs complex property table. It operates over set of RDF triples,
 * collects and transforms information about them into a table. If we have a list of
 * predicates/properties p1, ... , pN, then the scheme of the table is (s: STRING, p1:
 * LIST<STRING> OR STRING, ..., pN: LIST<STRING> OR STRING). Column s contains subjects.
 * For each subject , there is only one row in the table. Each predicate can be of complex
 * or simple type. If a predicate is of simple type means that there is no subject which
 * has more than one triple containing this property/predicate. Then the predicate column
 * is of type STRING. Otherwise, if a predicate is of complex type which means that there
 * exists at least one subject which has more than one triple containing this
 * property/predicate. Then the predicate column is of type LIST<STRING>.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class WidePropertyTableLoader extends Loader {

	protected String hdfs_input_directory;

	private final String tablename_properties = "properties";

	/**
	 * Separator used internally to distinguish two values in the same string.
	 */
	public String columns_separator = "\\$%";

	protected String output_db_name;
	protected String output_tablename = "wide_property_table";
	public String column_name_subject = super.column_name_subject;
	public String column_name_object = super.column_name_object;
	protected boolean wptPartitionedBySub = false;
	private boolean isInversePropertyTable = false;
	private boolean isJoinedTable = false;

	public WidePropertyTableLoader(final String hdfs_input_directory, final String database_name,
			final SparkSession spark, final boolean wptPartitionedBySub) {
		super(hdfs_input_directory, database_name, spark);
		this.wptPartitionedBySub = wptPartitionedBySub;

	}

	public WidePropertyTableLoader(final String hdfs_input_directory, final String database_name,
			final SparkSession spark, final boolean wptPartitionedBySub, final boolean isInversePropertyTable,
			final boolean isJoinedTable) {
		super(hdfs_input_directory, database_name, spark);
		this.wptPartitionedBySub = wptPartitionedBySub;

		this.isJoinedTable = isJoinedTable;
		if (!isJoinedTable) {
			this.isInversePropertyTable = isInversePropertyTable;
			if (isInversePropertyTable) {
				output_tablename = "inverse_" + output_tablename;
				final String temp = column_name_subject;
				column_name_subject = column_name_object;
				column_name_object = temp;
			}
		} else {
			output_tablename = "joined_property_table";
		}
	}

	@Override
	public void load() {
		if (isJoinedTable) {
			isInversePropertyTable = false;
			final Dataset<Row> wpt = loadDataset();
			final String temp = column_name_subject;
			column_name_subject = column_name_object;
			column_name_object = temp;
			isInversePropertyTable = true;
			final Dataset<Row> iwpt = loadDataset();

			final Dataset<Row> joinedPT = wpt.join(iwpt, wpt.col("s").equalTo(iwpt.col("o")), "outer");
			saveTable(joinedPT);

		} else {
			saveTable(loadDataset());
		}
	}

	/**
	 * This method handles the problem when two predicate are the same in a case-insensitive
	 * context but different in a case-sensitve one. For instance:
	 * <http://example.org/somename> and <http://example.org/someName>. Since Hive is case
	 * insensitive the problem will be solved removing one of the entries from the list of
	 * predicates.
	 */
	public Map<String, Boolean> handleCaseInsPredAndCard(final Map<String, Boolean> propertiesMultivaluesMap) {
		final Set<String> seenPredicates = new HashSet<>();
		final Set<String> originalRemovedPredicates = new HashSet<>();

		final Iterator<String> it = propertiesMultivaluesMap.keySet().iterator();
		while (it.hasNext()) {
			final String predicate = it.next();
			if (seenPredicates.contains(predicate.toLowerCase())) {
				originalRemovedPredicates.add(predicate);
			} else {
				seenPredicates.add(predicate.toLowerCase());
			}
		}

		for (final String predicateToBeRemoved : originalRemovedPredicates) {
			propertiesMultivaluesMap.remove(predicateToBeRemoved);
		}

		if (originalRemovedPredicates.size() > 0) {
			logger.info("The following predicates had to be removed from the list of predicates "
					+ "(it is case-insensitive equal to another predicate): " + originalRemovedPredicates);
		}
		return propertiesMultivaluesMap;
	}

	public void buildPropertiesAndCardinalities() {
		// return rows of format <predicate, is_complex>
		// is_complex can be 1 or 0
		// 1 for multivalued predicate, 0 for single predicate
		// select all the properties
		final Dataset<Row> allProperties = spark
				.sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s", column_name_predicate, name_tripletable));

		logger.info("Total Number of Properties found: " + allProperties.count());

		// select the properties that are multivalued
		final Dataset<Row> multivaluedProperties = spark.sql(String.format(
				"SELECT DISTINCT(%1$s) AS %1$s FROM "
						+ "(SELECT %2$s, %1$s, COUNT(*) AS rc FROM %3$s GROUP BY %2$s, %1$s HAVING rc > 1) AS grouped",
				column_name_predicate, column_name_subject, name_tripletable));

		logger.info("Number of Multivalued Properties found: " + multivaluedProperties.count());

		// select the properties that are not multivalued
		final Dataset<Row> singledValueProperties = allProperties.except(multivaluedProperties);
		logger.info("Number of Single-valued Properties found: " + singledValueProperties.count());

		// combine them
		final Dataset<Row> combinedProperties =
				singledValueProperties.selectExpr(column_name_predicate, "0 AS is_complex")
						.union(multivaluedProperties.selectExpr(column_name_predicate, "1 AS is_complex"));

		// remove '<' and '>', convert the characters
		final Dataset<Row> cleanedProperties = combinedProperties.withColumn("p",
				functions.regexp_replace(functions.translate(combinedProperties.col("p"), "<>", ""), "[[^\\w]+]", "_"));

		final List<Tuple2<String, Integer>> cleanedPropertiesList =
				cleanedProperties.as(Encoders.tuple(Encoders.STRING(), Encoders.INT())).collectAsList();
		if (cleanedPropertiesList.size() > 0) {
			logger.info("Clean Properties (stored): " + cleanedPropertiesList);
		}

		// write the result
		cleanedProperties.write().mode(SaveMode.Overwrite).saveAsTable("properties");
	}

	/**
	 * Create the final property table, allProperties contains the list of all possible
	 * properties isMultivaluedProperty contains (in the same order used by allProperties) the
	 * boolean value that indicates if that property is multi-valued or not.
	 */
	private Dataset<Row> buildWidePropertyTable(final String[] allProperties, final Boolean[] isMultivaluedProperty) {
		logger.info("Building the complete property table.");

		// create a new aggregation environment
		final PropertiesAggregateFunction aggregator =
				new PropertiesAggregateFunction(allProperties, columns_separator);

		final String predicateObjectColumn = "po";
		final String groupColumn = "group";

		// get the compressed table
		final Dataset<Row> compressedTriples = spark.sql(String.format("SELECT %s, CONCAT(%s, '%s', %s) AS po FROM %s",
				column_name_subject, column_name_predicate, columns_separator, column_name_object, name_tripletable));

		// group by the subject and get all the data
		final Dataset<Row> grouped = compressedTriples.groupBy(column_name_subject)
				.agg(aggregator.apply(compressedTriples.col(predicateObjectColumn)).alias(groupColumn));

		// build the query to extract the property from the array
		final String[] selectProperties = new String[allProperties.length + 1];
		selectProperties[0] = column_name_subject;
		for (int i = 0; i < allProperties.length; i++) {

			// if property is a full URI, remove the < at the beginning end > at
			// the end
			final String rawProperty = allProperties[i].startsWith("<") && allProperties[i].endsWith(">")
					? allProperties[i].substring(1, allProperties[i].length() - 1)
					: allProperties[i];
			// if is not a complex type, extract the value
			final String newProperty = isMultivaluedProperty[i]
					? " " + groupColumn + "[" + String.valueOf(i) + "] AS " + getValidHiveName(rawProperty)
					: " " + groupColumn + "[" + String.valueOf(i) + "][0] AS " + getValidHiveName(rawProperty);
			selectProperties[i + 1] = newProperty;
		}

		final List<String> allPropertiesList = Arrays.asList(selectProperties);
		logger.info("Columns of  Property Table: " + allPropertiesList);

		Dataset<Row> propertyTable = grouped.selectExpr(selectProperties);

		// renames the column so that its name is consistent with the non inverse Wide Property
		// Table.This guarantees that any method that access a Property Table can be used with a
		// Inverse Property Table without any changes
		if (isInversePropertyTable) {
			// propertyTable = propertyTable.withColumnRenamed(column_name_subject,
			// column_name_object);

			for (final String property : allProperties) {
				propertyTable = propertyTable.withColumnRenamed(property, "s_".concat(property));
			}
		} else {
			for (final String property : allProperties) {
				propertyTable = propertyTable.withColumnRenamed(property, "o_".concat(property));
			}
		}

		return propertyTable;

		// List<Row> sampledRowsList = propertyTable.limit(10).collectAsList();
		// logger.info("First 10 rows sampled from the PROPERTY TABLE (or less
		// if there
		// are less): " + sampledRowsList);

		/*
		 * //This code is to create a TT partitioned by subject with a fixed number of
		 * partiitions. //Run the code with: //Delete after results are there.
		 * logger.info("Number of partitions of WPT  before repartitioning: " +
		 * propertyTable.rdd().getNumPartitions()); Dataset<Row> propertyTable1000 =
		 * propertyTable.repartition(1000, propertyTable.col(column_name_subject));
		 * propertyTable1000.write().saveAsTable("wpt_partBySub_1000");
		 * logger.info("Number of partitions after repartitioning: " +
		 * propertyTable1000.rdd().getNumPartitions());
		 *
		 * logger.info("Number of partitions of WPT  before repartitioning: " +
		 * propertyTable.rdd().getNumPartitions()); Dataset<Row> propertyTable500 =
		 * propertyTable.repartition(500, propertyTable.col(column_name_subject));
		 * propertyTable500.write().saveAsTable("wpt_partBySub_500");
		 * logger.info("Number of partitions after repartitioning: " +
		 * propertyTable500.rdd().getNumPartitions());
		 *
		 * logger.info("Number of partitions of WPT  before repartitioning: " +
		 * propertyTable.rdd().getNumPartitions()); Dataset<Row> propertyTable100 =
		 * propertyTable.repartition(100, propertyTable.col(column_name_subject));
		 * propertyTable100.write().saveAsTable("wpt_partBySub_100");
		 * logger.info("Number of partitions after repartitioning: " +
		 * propertyTable100.rdd().getNumPartitions());
		 *
		 * logger.info("Number of partitions of WPT  before repartitioning: " +
		 * propertyTable.rdd().getNumPartitions()); Dataset<Row> propertyTable25 =
		 * propertyTable.repartition(25, propertyTable.col(column_name_subject));
		 * propertyTable25.write().saveAsTable("wpt_partBySub_25");
		 * logger.info("Number of partitions after repartitioning: " +
		 * propertyTable25.rdd().getNumPartitions());
		 *
		 * logger.info("Number of partitions of WPT  before repartitioning: " +
		 * propertyTable.rdd().getNumPartitions()); Dataset<Row> propertyTable10 =
		 * propertyTable.repartition(10, propertyTable.col(column_name_subject));
		 * propertyTable10.write().saveAsTable("wpt_partBySub_10");
		 * logger.info("Number of partitions after repartitioning: " +
		 * propertyTable10.rdd().getNumPartitions());
		 */
	}

	private Dataset<Row> loadDataset() {
		logger.info("PHASE 2: creating the property table...");

		buildPropertiesAndCardinalities();

		// collect information for all properties
		final List<Row> props = spark.sql(String.format("SELECT * FROM %s", tablename_properties)).collectAsList();
		String[] allProperties = new String[props.size()];
		Boolean[] isMultivaluedProperty = new Boolean[props.size()];

		for (int i = 0; i < props.size(); i++) {
			allProperties[i] = props.get(i).getString(0);
			isMultivaluedProperty[i] = props.get(i).getInt(1) == 1;
		}

		// We create a map with the properties and the boolean.
		final Map<String, Boolean> propertiesMultivaluesMap = new HashMap<>();
		for (int i = 0; i < allProperties.length; i++) {
			final String property = allProperties[i];
			final Boolean multivalued = isMultivaluedProperty[i];
			propertiesMultivaluesMap.put(property, multivalued);
		}

		final Map<String, Boolean> fixedPropertiesMultivaluesMap = handleCaseInsPredAndCard(propertiesMultivaluesMap);

		final List<String> allPropertiesList = new ArrayList<>();
		final List<Boolean> isMultivaluedPropertyList = new ArrayList<>();
		allPropertiesList.addAll(fixedPropertiesMultivaluesMap.keySet());

		for (int i = 0; i < allPropertiesList.size(); i++) {
			final String property = allPropertiesList.get(i);
			isMultivaluedPropertyList.add(fixedPropertiesMultivaluesMap.get(property));
		}

		logger.info("All properties as array: " + allPropertiesList);
		logger.info("Multi-values flag as array: " + isMultivaluedPropertyList);

		allProperties = allPropertiesList.toArray(new String[allPropertiesList.size()]);
		properties_names = allProperties;
		isMultivaluedProperty = isMultivaluedPropertyList.toArray(new Boolean[allPropertiesList.size()]);

		// create wide property table
		return buildWidePropertyTable(allProperties, isMultivaluedProperty);
	}

	private void saveTable(final Dataset<Row> propertyTableDataset) {
		// write the final one, partitioned by subject
		// propertyTable = propertyTable.repartition(1000, column_name_subject);
		if (wptPartitionedBySub) {
			if (isInversePropertyTable) {
				propertyTableDataset.write().mode(SaveMode.Overwrite).partitionBy(column_name_object)
						.format(table_format).saveAsTable(output_tablename);
			} else {
				propertyTableDataset.write().mode(SaveMode.Overwrite).partitionBy(column_name_subject)
						.format(table_format).saveAsTable(output_tablename);
			}
		} else if (!wptPartitionedBySub) {
			propertyTableDataset.write().mode(SaveMode.Overwrite).format(table_format).saveAsTable(output_tablename);
		}
		logger.info("Created property table with name: " + output_tablename);

	}
}